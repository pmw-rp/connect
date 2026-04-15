// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcd

import (
	"context"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	oofFieldInput         = "input"
	oofFieldEtcdEndpoints = "etcd_endpoints"
	oofFieldLockKey       = "lock_key"
	oofFieldLeaseTTL      = "lease_ttl"

	dialTimeout   = 5 * time.Second
	retryDelay    = 5 * time.Second
	resignTimeout = 5 * time.Second

	// keepaliveTimeoutFraction is the fraction of lease_ttl after which, if no
	// keepalive ACK has been received, we voluntarily step down. This ensures
	// we stop reading before the lease expires and a standby can win, preventing
	// two instances from reading concurrently.
	keepaliveTimeoutFraction = 2
)

func oneOfInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("4.44.0").
		Categories("Utility").
		Summary("Wraps a child input with etcd-based distributed leader election, ensuring only one instance reads at a time.").
		Description(`
The `+"`one_of`"+` input is designed for high-availability deployments where multiple pipeline
instances run the same configuration but only one should pull data at a time. This is
critical for ordered sources such as CDC (Change Data Capture) inputs where concurrent
reads would result in out-of-order messages.

Each instance competes for an etcd distributed lease. The winner reads from the child
input; standby instances block. If the active instance's lease expires (due to a crash
or network partition), a standby instance wins the election and resumes reading within
the configured `+"`lease_ttl`"+`.

On graceful shutdown the leader resigns immediately, allowing a standby to take over
before the TTL expires.
`).
		Fields(
			service.NewInputField(oofFieldInput).
				Description("The child input to gate behind the distributed lock. Only the elected leader reads from this input."),
			service.NewStringListField(oofFieldEtcdEndpoints).
				Description("List of etcd server endpoints.").
				Example([]any{"http://etcd:2379"}),
			service.NewStringField(oofFieldLockKey).
				Description("The etcd key prefix used for leader election. All competing instances must use the same key.").
				Example("/my-cdc-lock"),
			service.NewDurationField(oofFieldLeaseTTL).
				Description("How long the etcd lease survives without a keepalive heartbeat. If the leader process crashes, standby instances will take over within this duration.").
				Default("15s").
				Advanced(),
		)
}

func init() {
	service.MustRegisterBatchInput("one_of", oneOfInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newOneOfInput(conf, mgr)
		})
}

type leaderState int

const (
	stateFollower leaderState = iota
	stateLeader
	stateClosed
)

type oneOfInput struct {
	// Immutable after construction.
	endpoints []string
	lockKey   string
	leaseTTL  int // seconds
	child     *service.OwnedInput
	log       *service.Logger

	// Mutable state, protected by mu.
	mu             sync.Mutex
	state          leaderState
	leaderCh       chan struct{} // closed=leader (unblocks ReadBatch); open=follower (blocks)
	electionCancel context.CancelFunc
	electionDone   chan struct{} // closed when electionLoop exits
	etcdClient     *clientv3.Client
}

func newOneOfInput(conf *service.ParsedConfig, mgr *service.Resources) (*oneOfInput, error) {
	o := &oneOfInput{
		log:      mgr.Logger(),
		leaderCh: make(chan struct{}), // starts open (blocking) — not yet leader
		state:    stateFollower,
	}
	var err error
	if o.endpoints, err = conf.FieldStringList(oofFieldEtcdEndpoints); err != nil {
		return nil, err
	}
	if o.lockKey, err = conf.FieldString(oofFieldLockKey); err != nil {
		return nil, err
	}
	ttl, err := conf.FieldDuration(oofFieldLeaseTTL)
	if err != nil {
		return nil, err
	}
	o.leaseTTL = int(ttl.Seconds())
	if o.leaseTTL < 1 {
		o.leaseTTL = 1
	}
	if o.child, err = conf.FieldInput(oofFieldInput); err != nil {
		return nil, err
	}
	return o, nil
}

// Connect establishes the etcd connection and starts the background election loop.
// The child input manages its own connection internally.
func (o *oneOfInput) Connect(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.state == stateClosed {
		return service.ErrNotConnected
	}
	if o.etcdClient != nil {
		return nil // already connected
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   o.endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		return fmt.Errorf("one_of etcd connect: %w", err)
	}
	o.etcdClient = cli

	// Kick off the child input's internal loop by calling ReadBatch with an
	// already-cancelled context. This starts the child's async reader goroutine
	// without consuming any messages, which is necessary so that child.Close()
	// can later call WaitForClose() and have it return (WaitForClose blocks
	// forever if the loop was never started).
	cancelledCtx, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	_, _, _ = o.child.ReadBatch(cancelledCtx)

	// The election context lives for the lifetime of this input, cancelled by Close.
	electionCtx, cancel := context.WithCancel(context.Background())
	o.electionCancel = cancel
	o.electionDone = make(chan struct{})
	go func() {
		defer close(o.electionDone)
		o.electionLoop(electionCtx)
	}()

	return nil
}

// ReadBatch blocks until this instance holds the etcd lease, then delegates to the child input.
func (o *oneOfInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	for {
		// Snapshot leaderCh under lock; do not hold lock while blocking.
		o.mu.Lock()
		if o.state == stateClosed {
			o.mu.Unlock()
			return nil, nil, service.ErrEndOfInput
		}
		if o.etcdClient == nil {
			o.mu.Unlock()
			return nil, nil, service.ErrNotConnected
		}
		ch := o.leaderCh
		o.mu.Unlock()

		// Block until leader or context cancelled.
		select {
		case <-ch:
			// We are (or became) leader; proceed.
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}

		batch, ackFn, err := o.child.ReadBatch(ctx)
		if err != nil {
			// If leaderCh was replaced while we were blocked in ReadBatch, we lost
			// leadership. Loop back to wait for re-election rather than propagating
			// the error, which may have been caused by the leadership transition.
			o.mu.Lock()
			currentCh := o.leaderCh
			o.mu.Unlock()
			if currentCh != ch {
				o.log.Warn("one_of: lost leadership during read, waiting to re-acquire")
				continue
			}
			return nil, nil, err
		}

		// Acks propagate transparently — the child's AckFunc is returned directly.
		return batch, ackFn, nil
	}
}

// Close resigns from the election (if leader), shuts down the election loop, and closes the child input.
func (o *oneOfInput) Close(ctx context.Context) error {
	o.mu.Lock()
	if o.state == stateClosed {
		o.mu.Unlock()
		return nil
	}
	wasFollower := o.state == stateFollower
	o.state = stateClosed
	cancel := o.electionCancel
	done := o.electionDone
	cli := o.etcdClient
	// Only close leaderCh if it hasn't already been closed by becomeLeader.
	// A follower's leaderCh is open (blocking); closing it unblocks ReadBatch
	// so it can observe stateClosed and return ErrEndOfInput.
	// A leader's leaderCh is already closed, so we must not close it again.
	if wasFollower {
		close(o.leaderCh)
	}
	o.mu.Unlock()

	if cancel != nil {
		cancel() // signals electionLoop to resign and exit
	}

	// Wait for electionLoop to finish the Resign RPC before closing the etcd
	// client — closing the connection mid-Resign causes spurious errors.
	// Fall through on ctx cancellation to avoid blocking forever.
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
		}
	}

	if cli != nil {
		_ = cli.Close()
	}

	return o.child.Close(ctx)
}

// electionLoop is a long-lived goroutine that continuously attempts to win the
// etcd leader election. It runs from Connect until Close.
//
// To prevent two instances reading concurrently during a network partition, we
// manage the lease keepalive channel ourselves. If no keepalive ACK arrives
// within lease_ttl/2, we step down proactively — before the lease expires and
// before a standby can win. This bounds the split-brain window to zero under
// normal conditions and to the keepalive timeout under partition conditions,
// rather than the full lease_ttl.
func (o *oneOfInput) electionLoop(ctx context.Context) {
	keepaliveTimeout := time.Duration(o.leaseTTL) * time.Second / keepaliveTimeoutFraction

	for {
		if ctx.Err() != nil {
			return
		}

		o.log.Debug("one_of: granting lease, attempting campaign")

		// Phase 1: Grant a lease manually so we can monitor its keepalive channel.
		grantResp, err := o.etcdClient.Grant(ctx, int64(o.leaseTTL))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			o.log.Warnf("one_of: lease grant error: %v; retrying in 5s", err)
			select {
			case <-time.After(retryDelay):
			case <-ctx.Done():
				return
			}
			continue
		}
		leaseID := grantResp.ID

		// Phase 2: Start keepalive — we own this channel and watch it directly.
		keepaliveCh, err := o.etcdClient.KeepAlive(ctx, leaseID)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			o.log.Warnf("one_of: keepalive error: %v; retrying in 5s", err)
			_, _ = o.etcdClient.Revoke(ctx, leaseID)
			select {
			case <-time.After(retryDelay):
			case <-ctx.Done():
				return
			}
			continue
		}

		// Phase 3: Create a session from our lease and campaign.
		sess, err := concurrency.NewSession(o.etcdClient,
			concurrency.WithLease(leaseID),
			concurrency.WithContext(ctx),
		)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			o.log.Warnf("one_of: session error: %v; retrying in 5s", err)
			_, _ = o.etcdClient.Revoke(ctx, leaseID)
			select {
			case <-time.After(retryDelay):
			case <-ctx.Done():
				return
			}
			continue
		}

		election := concurrency.NewElection(sess, o.lockKey)
		if err := election.Campaign(ctx, "1"); err != nil {
			_ = sess.Close()
			if ctx.Err() != nil {
				return
			}
			o.log.Warnf("one_of: campaign error: %v; retrying", err)
			continue
		}

		// Phase 4: We are now leader. Activate the child input.
		o.log.Info("one_of: acquired leadership, activating child input")
		o.becomeLeader()

		// Phase 5: Watch keepalive responses while leader.
		// If no ACK arrives within lease_ttl/2 we step down proactively,
		// guaranteeing we stop reading before the lease expires on etcd's side.
		lostLeadership := o.watchKeepalive(ctx, keepaliveCh, keepaliveTimeout, sess)

		// Phase 6: Step down.
		o.becomeFollower()

		if !lostLeadership {
			// Graceful shutdown: resign so the standby wins immediately.
			resignCtx, resignCancel := context.WithTimeout(context.Background(), resignTimeout)
			if resignErr := election.Resign(resignCtx); resignErr != nil {
				o.log.Warnf("one_of: resign error: %v", resignErr)
			}
			resignCancel()
			_ = sess.Close()
			return
		}

		// Keepalive timeout or session expiry: revoke lease explicitly so the
		// standby doesn't have to wait for TTL expiry on etcd's side either.
		o.log.Warn("one_of: stepping down due to keepalive timeout, revoking lease")
		_ = sess.Close()
		revokeCtx, revokeCancel := context.WithTimeout(context.Background(), resignTimeout)
		_, _ = o.etcdClient.Revoke(revokeCtx, leaseID)
		revokeCancel()
		// Loop back immediately to re-campaign.
	}
}

// watchKeepalive monitors the keepalive channel while this instance is leader.
// Returns false if ctx was cancelled (graceful shutdown), true if leadership
// was lost due to keepalive timeout or session expiry.
func (o *oneOfInput) watchKeepalive(ctx context.Context, keepaliveCh <-chan *clientv3.LeaseKeepAliveResponse, timeout time.Duration, sess *concurrency.Session) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case resp, ok := <-keepaliveCh:
			if !ok || resp == nil {
				// keepalive channel closed — lease expired or etcd unreachable.
				o.log.Warn("one_of: keepalive channel closed, releasing leadership")
				return true
			}
			// ACK received: reset the timeout.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(timeout)

		case <-timer.C:
			// No ACK within lease_ttl/2 — step down proactively before etcd expires us.
			o.log.Warnf("one_of: no keepalive ACK for %v, stepping down to prevent split-brain", timeout)
			return true

		case <-sess.Done():
			o.log.Warn("one_of: etcd session expired, releasing leadership")
			return true

		case <-ctx.Done():
			return false
		}
	}
}

func (o *oneOfInput) becomeLeader() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.state == stateClosed {
		return
	}
	o.state = stateLeader
	close(o.leaderCh) // unblocks all goroutines blocked in ReadBatch
}

func (o *oneOfInput) becomeFollower() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.state == stateClosed {
		return
	}
	o.state = stateFollower
	o.leaderCh = make(chan struct{}) // new blocking channel; ReadBatch will re-block
}
