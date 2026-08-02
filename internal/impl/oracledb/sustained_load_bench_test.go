// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb_test

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	sustainedRootConnStr       = "oracle://c%23%23testdb:testdb123@localhost:1521/FREE"
	sustainedPDBConnStr        = "oracle://c%23%23testdb:testdb123@localhost:1521/TESTPDB"
	sustainedTargetBytesPerSec = 20.0 * 1024 * 1024 // ~20 MiB/s of sustained redo generation
	sustainedDuration          = 10 * time.Minute
	sustainedDrainGrace        = 3 * time.Minute
	sustainedProgressInterval  = 15 * time.Second
	sustainedPayloadFill       = 150

	// The generator cycles UPDATEs through a fixed-size row pool instead of ever INSERTing new
	// rows. Oracle Database Free hard-caps total user data at 12GB (ORA-12954) and does not allow
	// raising it; a pure-INSERT generator sustaining 20 MiB/s exhausts that budget in well under
	// 15 minutes regardless of cleanup between runs. A fixed pool keeps table size constant
	// forever, so duration and rate are limited only by redo/archive throughput, not table growth.
	sustainedPoolChunkSize = 3000
	sustainedPoolChunks    = 100
	sustainedPoolSize      = sustainedPoolChunkSize * sustainedPoolChunks
)

// TestSustainedLoad measures whether the connector's throughput and per-event latency stay
// bounded under a long, steady, realistic change rate, rather than the short backlog-catchup
// bursts other benchmarks in this package measure. Lag that grows or spikes under sustained load
// is exactly the failure mode manual LogMiner batch/sleep tuning is prone to.
func TestSustainedLoad(t *testing.T) {
	runSustainedLoadBenchmark(t, "sustained", `
    scn_window_size: 20000
    min_scn_window_size: 1000
    max_scn_window_size: 100000`)
}

func runSustainedLoadBenchmark(t *testing.T, strategyName, logminerCfg string) {
	integration.CheckSkip(t)

	rawDB, err := sql.Open("oracle", sustainedPDBConnStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rawDB.Close() })
	require.NoError(t, rawDB.PingContext(t.Context()))

	tableName := "bench_" + strategyName
	seqName := tableName + "_seq"
	cacheKey := "sustained1-" + strategyName

	_, err = rawDB.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE testdb.%s", tableName))
	if err != nil {
		t.Logf("drop %s (expected on first run): %v", tableName, err)
	}
	_, err = rawDB.ExecContext(t.Context(), fmt.Sprintf("DROP SEQUENCE testdb.%s", seqName))
	if err != nil {
		t.Logf("drop %s (expected on first run): %v", seqName, err)
	}
	_, err = rawDB.ExecContext(t.Context(), fmt.Sprintf(`CREATE TABLE testdb.%s (
		id NUMBER PRIMARY KEY,
		seq NUMBER DEFAULT 0,
		payload VARCHAR2(200)
	)`, tableName))
	require.NoError(t, err)
	_, err = rawDB.ExecContext(t.Context(), fmt.Sprintf("ALTER TABLE testdb.%s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS", tableName))
	require.NoError(t, err)
	_, err = rawDB.ExecContext(t.Context(), fmt.Sprintf("CREATE SEQUENCE testdb.%s START WITH 1 INCREMENT BY 1 CACHE 10000", seqName))
	require.NoError(t, err)

	// Pre-populate the fixed row pool. This happens before the connector starts, so none of it is
	// ever seen as change events - only the UPDATEs that cycle through it afterward are.
	const popBatch = 10000
	for start := 1; start <= sustainedPoolSize; start += popBatch {
		n := popBatch
		if start+n-1 > sustainedPoolSize {
			n = sustainedPoolSize - start + 1
		}
		_, err = rawDB.ExecContext(t.Context(), fmt.Sprintf(`
			INSERT INTO testdb.%s (id, seq, payload)
			SELECT :1 + ROWNUM - 1, 0, LPAD('x', %d, 'x') FROM dual CONNECT BY LEVEL <= :2`,
			tableName, sustainedPayloadFill), start, n)
		require.NoError(t, err)
	}

	// Calibrate actual redo bytes/row on a dedicated session (V$MYSTAT is per-session) BEFORE
	// starting the connector, so the calibration UPDATE is historical by the time it starts
	// tailing from the current SCN and is never seen by it. This consumes the sequence's first
	// sustainedPoolChunkSize values; the sustained phase's tracked range starts right after.
	calibConn, err := rawDB.Conn(t.Context())
	require.NoError(t, err)
	redoStat := func() int64 {
		var v int64
		require.NoError(t, calibConn.QueryRowContext(t.Context(), `
			SELECT b.value FROM v$mystat b JOIN v$statname n ON b.statistic# = n.statistic#
			WHERE n.name = 'redo size'`).Scan(&v))
		return v
	}
	before := redoStat()
	_, err = calibConn.ExecContext(t.Context(), fmt.Sprintf(`
		UPDATE testdb.%s SET seq = testdb.%s.NEXTVAL, payload = LPAD('x', %d, 'x')
		WHERE id BETWEEN 1 AND %d`, tableName, seqName, sustainedPayloadFill, sustainedPoolChunkSize))
	require.NoError(t, err)
	after := redoStat()
	require.NoError(t, calibConn.Close())

	bytesPerRow := float64(after-before) / float64(sustainedPoolChunkSize)
	rowsPerSec := sustainedTargetBytesPerSec / bytesPerRow
	chunksPerSec := rowsPerSec / float64(sustainedPoolChunkSize)
	chunkInterval := time.Duration(float64(time.Second) / chunksPerSec)
	t.Logf("[%s] calibration: %.1f redo bytes/row -> targeting %.0f rows/sec (chunk=%d every %s)",
		strategyName, bytesPerRow, rowsPerSec, sustainedPoolChunkSize, chunkInterval)

	cfg := fmt.Sprintf(`
oracledb_cdc:
  connection_string: %s
  pdb_name: TESTPDB
  snapshot_mode: none
  checkpoint_cache_key: %s
  logminer:
    backoff_interval: 300ms
    mining_interval: 100ms
%s
  include: ["TESTDB.%s"]
  batching:
    count: 1000
    period: 200ms`, sustainedRootConnStr, cacheKey, logminerCfg, sustainedUpper(tableName))

	// Sequence values for the sustained phase start right after the calibration UPDATE's range.
	baseSeq := sustainedPoolChunkSize + 1
	maxExpectedRows := int(rowsPerSec*sustainedDuration.Seconds()*1.5) + 10000
	bits := newIDBitset(baseSeq, maxExpectedRows)

	var delivered atomic.Int64
	var duplicates atomic.Int64
	var lastLatencyNanos atomic.Int64
	var latMu sync.Mutex
	latencies := make([]time.Duration, 0, maxExpectedRows)

	builder := service.NewStreamBuilder()
	require.NoError(t, builder.AddInputYAML(cfg))
	require.NoError(t, builder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		delivered.Add(1)
		if structured, err := msg.AsStructured(); err == nil {
			if m, ok := structured.(map[string]any); ok {
				if seq, err := strconv.Atoi(fmt.Sprint(m["SEQ"])); err == nil {
					if alreadySet := bits.set(seq); alreadySet {
						duplicates.Add(1)
					}
				}
			}
		}
		if ts, ok := msg.MetaGet("commit_ts_ms"); ok {
			if ms, err := strconv.ParseInt(ts, 10, 64); err == nil {
				lat := time.Since(time.UnixMilli(ms))
				latMu.Lock()
				latencies = append(latencies, lat)
				latMu.Unlock()
				lastLatencyNanos.Store(int64(lat))
			}
		}
		return nil
	}))
	stream, err := builder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- stream.Run(runCtx) }()

	// Give the connector time to establish its starting SCN before generating changes.
	time.Sleep(2 * time.Second)

	genStart := time.Now()
	deadline := genStart.Add(sustainedDuration)
	nextTick := genStart
	nextProgress := genStart.Add(sustainedProgressInterval)
	totalUpdated := 0
	chunkIndex := 0
	for time.Now().Before(deadline) {
		chunkStart := chunkIndex*sustainedPoolChunkSize + 1
		chunkEnd := chunkStart + sustainedPoolChunkSize - 1
		_, err := rawDB.ExecContext(t.Context(), fmt.Sprintf(`
			UPDATE testdb.%s SET seq = testdb.%s.NEXTVAL, payload = LPAD('x', %d, 'x')
			WHERE id BETWEEN :1 AND :2`, tableName, seqName, sustainedPayloadFill), chunkStart, chunkEnd)
		require.NoError(t, err)
		totalUpdated += sustainedPoolChunkSize
		chunkIndex = (chunkIndex + 1) % sustainedPoolChunks

		if now := time.Now(); now.After(nextProgress) {
			d := delivered.Load()
			t.Logf("[%s] +%s updated=%d delivered=%d duplicates=%d backlog=%d last_latency=%s",
				strategyName, now.Sub(genStart).Round(time.Second), totalUpdated, d, duplicates.Load(),
				int64(totalUpdated)-d, time.Duration(lastLatencyNanos.Load()))
			nextProgress = now.Add(sustainedProgressInterval)
		}

		nextTick = nextTick.Add(chunkInterval)
		if sleepFor := time.Until(nextTick); sleepFor > 0 {
			time.Sleep(sleepFor)
		}
	}
	actualElapsed := time.Since(genStart)
	actualRate := float64(totalUpdated) * bytesPerRow / actualElapsed.Seconds()
	t.Logf("[%s] generation done: %d updates in %s (target %.0f rows/sec, actual %.0f rows/sec, ~%.2f MiB/s)",
		strategyName, totalUpdated, actualElapsed, rowsPerSec, float64(totalUpdated)/actualElapsed.Seconds(), actualRate/1024/1024)

	drainDeadline := time.Now().Add(sustainedDrainGrace)
	for time.Now().Before(drainDeadline) && bits.count(totalUpdated) < totalUpdated {
		time.Sleep(2 * time.Second)
	}

	gotCount := bits.count(totalUpdated)
	missing := bits.missing(totalUpdated)

	cancel()
	<-runDone

	latMu.Lock()
	sorted := append([]time.Duration(nil), latencies...)
	latMu.Unlock()
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pct := func(p float64) time.Duration {
		if len(sorted) == 0 {
			return 0
		}
		idx := int(p * float64(len(sorted)-1))
		return sorted[idx]
	}
	var maxLat time.Duration
	if len(sorted) > 0 {
		maxLat = sorted[len(sorted)-1]
	}

	t.Logf("RESULT %s: updated=%d delivered_ok=%d/%d missing=%d duplicates=%d actual_rate=%.2fMiB/s p50=%s p90=%s p99=%s max=%s",
		strategyName, totalUpdated, gotCount, totalUpdated, len(missing), duplicates.Load(), actualRate/1024/1024,
		pct(0.5), pct(0.9), pct(0.99), maxLat)

	if len(missing) > 0 {
		n := len(missing)
		if n > 20 {
			n = 20
		}
		t.Errorf("[%s] %d of %d updates never delivered after drain grace period; first missing seqs: %v",
			strategyName, len(missing), totalUpdated, missing[:n])
	}
}

func sustainedUpper(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 'a' + 'A'
		}
	}
	return string(b)
}

// idBitset tracks delivery of a contiguous, densely-packed range of sequence values far more
// cheaply than a map[int]bool at the millions-of-updates scale a multi-minute sustained run
// produces.
type idBitset struct {
	mu   sync.Mutex
	bits []uint64
	base int
}

func newIDBitset(base, n int) *idBitset {
	return &idBitset{bits: make([]uint64, n/64+1), base: base}
}

// set marks id as seen and reports whether it was already set (a duplicate delivery).
func (b *idBitset) set(id int) bool {
	idx := id - b.base
	if idx < 0 {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	word := idx / 64
	if word >= len(b.bits) {
		return false
	}
	mask := uint64(1) << uint(idx%64)
	already := b.bits[word]&mask != 0
	b.bits[word] |= mask
	return already
}

func (b *idBitset) count(n int) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := 0
	for i := 0; i < n; i++ {
		if b.bits[i/64]&(1<<uint(i%64)) != 0 {
			c++
		}
	}
	return c
}

func (b *idBitset) missing(n int) []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	var missing []int
	for i := 0; i < n; i++ {
		if b.bits[i/64]&(1<<uint(i%64)) == 0 {
			missing = append(missing, b.base+i)
			if len(missing) >= 50 {
				break
			}
		}
	}
	return missing
}
