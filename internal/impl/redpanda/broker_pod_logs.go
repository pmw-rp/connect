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

package redpanda

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Jeffail/shutdown"
	"github.com/ohler55/ojg/jp"
	"github.com/ohler55/ojg/oj"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
	"github.com/redpanda-data/connect/v4/internal/impl/kubernetes"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"io"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
	"time"
)

func podLogInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("3.61.0").
		Summary(`A Kubernetes pod log input using the https://github.com/kubernetes/client-go[Kubernetes client-go library^].`).
		Description("").
		Fields(PodLogInputConfigFields()...)
}

// PodLogInputConfigFields returns the full suite of config fields
func PodLogInputConfigFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField("namespace").
			Description("The namespace for the Redpanda pod").
			Example([]string{"redpanda"}),
		service.NewStringField("pod_prefix").
			Description("The prefix of the Redpanda pod").
			Example([]string{"redpanda-0"}),
		service.NewStringField("pod_name").
			Description("The name of this pod").
			Example([]string{"logs-forwarder-0"}),
		service.NewStringField("container").
			Description("The name of the container within the pod").
			Example([]string{"redpanda"}),
		service.NewTLSToggledField("tls"),
		kafka.SASLFields(),
		service.NewStringMapField("labels").
			Description("A map of labels to populate from the pod metadata"),
		service.NewIntField("batchSize").Optional().
			Description("The name of the container within the pod").
			Example([]string{"redpanda"}),
		service.NewBoolField("show_label_paths").Optional().Default(false).
			Description("Whether to iterate over all label paths to help during config creation."),
		service.NewAutoRetryNacksToggleField(),
	}
}

func init() {
	err := service.RegisterBatchInput("broker_pod_log", podLogInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			rdr, err := NewBrokerLogReaderFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatchedToggled(conf, rdr)
		})
	if err != nil {
		panic(err)
	}
}

// BrokerLogReader implements a log reader using the k8s-client library.
type BrokerLogReader struct {
	// to address the pod
	namespace string
	podPrefix string
	podName   string
	container string

	// to stream data
	batchSize int
	closing   bool
	stream    io.ReadCloser
	lines     chan string
	eof       bool
	count     int64

	// for redpanda
	SeedBrokers []string
	clientID    string
	TLSConf     *tls.Config
	saslConfs   []sasl.Mechanism
	nodeId      string
	clusterId   string

	// for label handling
	showLabelPaths bool
	labelPaths     map[string]string
	labels         map[string]string

	// for management
	res     *service.Resources
	log     *service.Logger
	shutSig *shutdown.Signaller
}

// NewBrokerLogReaderFromConfig attempts to instantiate a new BrokerLogReader from a parsed config.
func NewBrokerLogReaderFromConfig(conf *service.ParsedConfig, res *service.Resources) (*BrokerLogReader, error) {
	r := BrokerLogReader{
		closing: false,
		res:     res,
		log:     res.Logger(),
		shutSig: shutdown.NewSignaller(),
	}
	batchSize, err := conf.FieldInt("batchSize")
	if err != nil {
		r.batchSize = 1000
	} else {
		r.batchSize = batchSize
	}
	r.lines = make(chan string, r.batchSize)
	r.labels = make(map[string]string)

	r.showLabelPaths, err = conf.FieldBool("show_label_paths")
	if err != nil {
		panic(err)
	}

	r.labelPaths, err = conf.FieldStringMap("labels")
	if err != nil {
		panic(err)
	}

	r.namespace, err = conf.FieldString("namespace")
	if err != nil {
		panic(err)
	}

	r.podPrefix, err = conf.FieldString("pod_prefix")
	if err != nil {
		panic(err)
	}

	r.podName, err = conf.FieldString("pod_name")
	if err != nil {
		panic(err)
	}

	r.container, err = conf.FieldString("container")
	if err != nil {
		panic(err)
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled("tls")
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		r.TLSConf = tlsConf
	}
	if r.saslConfs, err = kafka.SASLMechanismsFromConfig(conf); err != nil {
		return nil, err
	}

	r.closing = false

	return &r, nil
}

func (r *BrokerLogReader) foreverPoll() error {
	scanner := bufio.NewScanner(r.stream)
	for scanner.Scan() {
		r.lines <- scanner.Text()
	}
	err := scanner.Err()
	if err != nil {
		panic(err)
	}

	return service.ErrEndOfInput
}

func (r *BrokerLogReader) popLine() (string, bool) {
	select {
	case line := <-r.lines:
		return line, true
	case <-time.After(10 * time.Millisecond):
		// call timed out
		return "", false
	}
}

func maybePanic(err error) {
	if err != nil {
		panic(err)
	}
}

func extract(path string, obj any) (string, error) {
	parsedPath, err := jp.ParseString(path)
	if err != nil {
		return "", err
	}
	results := parsedPath.Get(obj)
	if len(results) == 1 {
		return fmt.Sprintf("%v", results[0]), nil
	} else {
		return "", errors.New("oops")
	}
}

func (r *BrokerLogReader) populateRedpandaMetadata(ctx context.Context) error {
	r.log.Debugf("Connecting to %v", r.SeedBrokers)
	var cl *kgo.Client

	clientOpts := []kgo.Opt{
		kgo.SeedBrokers(r.SeedBrokers...),
		kgo.SASL(r.saslConfs...),
		kgo.ClientID(r.clientID),
	}

	if r.TLSConf != nil {
		clientOpts = append(clientOpts, kgo.DialTLSConfig(r.TLSConf))
	}

	var err error
	if cl, err = kgo.NewClient(clientOpts...); err != nil {
		return err
	}

	adm := kadm.NewClient(cl)

	r.log.Debug("Retrieving Redpanda metadata...")
	metadata, err := adm.BrokerMetadata(ctx)
	if err != nil {
		return err
	}

	r.clusterId = metadata.Cluster
	r.log.Debugf("Determined Redpanda cluster ID as %v", r.clusterId)
	for _, broker := range metadata.Brokers {
		host := fmt.Sprintf(broker.Host+":%v", broker.Port)
		if host == r.SeedBrokers[0] {
			r.nodeId = fmt.Sprintf("%v", broker.NodeID)
		}
	}

	cl.Close()
	r.log.Debug("Closed connection to Redpanda")

	return nil
}

func (r *BrokerLogReader) getPodName() (string, error) {
	parts := strings.Split(r.podName, "-")
	ordinal := parts[len(parts)-1]
	return r.podPrefix + "-" + ordinal, nil
}

// Connect to the pod logs.
func (r *BrokerLogReader) Connect(ctx context.Context) error {
	cs, err := kubernetes.GetClientSet()
	maybePanic(err)

	getOptions := metav1.GetOptions{
		TypeMeta:        metav1.TypeMeta{},
		ResourceVersion: "",
	}
	podName, err := r.getPodName()
	if err != nil {
		return err
	}
	pod, err := cs.CoreV1().Pods(r.namespace).Get(ctx, podName, getOptions)
	if err != nil {
		return err
	}

	b, _ := json.Marshal(pod)
	obj, err := oj.Parse(b)
	if err != nil {
		return err
	}

	if r.showLabelPaths {
		jp.Walk(obj, func(path jp.Expr, value any) {
			fmt.Printf("%v: %v\n", path, value)
		}, true)
	}

	kafkaPort, err := extract("$.spec.containers[?(@.command[0]=='rpk')].ports[?(@.name=='kafka')].containerPort", obj)
	if err != nil {
		return err
	}
	_ = kafkaPort

	adminPort, err := extract("$.spec.containers[?(@.command[0]=='rpk')].ports[?(@.name=='admin')].containerPort", obj)
	if err != nil {
		return err
	}
	_ = adminPort

	instance, err := extract("$.metadata.labels['app.kubernetes.io/instance']", obj)
	if err != nil {
		return err
	}

	host := podName + "." + instance + "." + r.namespace + ".svc.cluster.local."
	r.SeedBrokers = []string{host + ":" + kafkaPort}

	err = r.populateRedpandaMetadata(ctx)
	if err != nil {
		return err
	}

	for name, path := range r.labelPaths {
		parsedPath, err := jp.ParseString(path)
		if err != nil {
			panic(err)
		}
		result := parsedPath.Get(obj)
		r.labels[name] = fmt.Sprint(result[0])
	}

	r.labels["redpanda_node_id"] = r.nodeId
	r.labels["redpanda_id"] = r.clusterId

	podLogOptions := v1.PodLogOptions{
		Container: r.container,
		Follow:    true,
		TailLines: &r.count,
	}

	podLogRequest := cs.
		CoreV1().
		Pods(r.namespace).
		GetLogs(podName, &podLogOptions)
	stream, err := podLogRequest.Stream(context.TODO())

	if err != nil {
		return err
	}

	r.stream = stream
	go func() {
		err := r.foreverPoll()
		if errors.Is(err, service.ErrEndOfInput) {
			r.eof = true
			r.count = 10000 // Set the count so that when we reconnect, we pull any extra log lines from the start
		} else if err != nil {
			panic(err)
		}
	}()

	r.eof = false

	return nil
}

// See https://grafana.com/docs/loki/latest/send-data/promtail/stages/timestamp/
var lastTimestampForFudging time.Time

func extractTimestamp(s string, format string) (time.Time, bool) {
	ts, err := time.Parse(format, s)
	if err != nil {
		ts = lastTimestampForFudging.Add(1 * time.Millisecond)
		return ts, true
	}
	return ts, false
}

func (r *BrokerLogReader) addMeta(message *service.Message) error {

	// Add static labels found during connection
	for k, v := range r.labels {
		message.MetaSetMut(k, v)
	}

	content, err := message.AsBytes()
	if err != nil {
		return err
	}
	line := string(content)

	//
	firstSpace := strings.Index(line, " ")

	// Add log level
	if strings.HasPrefix(line, "INFO ") || strings.HasPrefix(line, "WARN ") || strings.HasPrefix(line, "DEBUG ") || strings.HasPrefix(line, "TRACE ") {
		level := line[:firstSpace]
		message.MetaSetMut("level", level)
	} else {
		message.MetaSetMut("level", "INFO")
	}

	// Add timestamp
	var ts time.Time
	var fudgedTs bool

	if len(line) >= firstSpace+25 {
		ts, fudgedTs = extractTimestamp(line[firstSpace+2:firstSpace+25], "2006-01-02 15:04:05,000")
	} else {
		ts, fudgedTs = extractTimestamp("oops", "2006-01-02 15:04:05,000")
	}

	lastTimestampForFudging = ts
	message.MetaSetMut("timestamp", ts.Format("2006-01-02 15:04:05,000"))

	// If we added a fudged timestamp (to preserve log ordering), mark it in the record
	if fudgedTs {
		message.MetaSetMut("timestamp_was_fudged", "true")
	}

	return nil
}

// ReadBatch attempts to read a batch of log messages from the target pod container.
func (r *BrokerLogReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {

	messages := service.MessageBatch{}

	more := true
	for more {
		line, ok := r.popLine()
		if ok {
			messages = append(messages, service.NewMessage([]byte(line)))
		} else {
			more = false
		}
		if len(messages) == r.batchSize {
			more = false
		}
	}

	for _, message := range messages {
		err := r.addMeta(message)
		if err != nil {
			return nil, nil, err
		}
	}

	if len(messages) == 0 && r.eof == true {
		return nil, nil, service.ErrNotConnected
	}

	return messages, func(ctx context.Context, res error) error {
		return nil
	}, nil
}

// Close underlying connections.
func (r *BrokerLogReader) Close(ctx context.Context) error {
	r.closing = true
	return nil
}
