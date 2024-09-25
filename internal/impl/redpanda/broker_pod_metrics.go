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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"net/http"
	"strings"
	"sync"
	"time"
)

func brokerPodMetricsInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("3.61.0").
		Summary(`A Kubernetes pod metrics input using the https://github.com/kubernetes/client-go[Kubernetes client-go library^].`).
		Description("").
		Fields(BrokerPodMetricsInputConfigFields()...)
}

// BrokerPodMetricsInputConfigFields returns the full suite of config fields
func BrokerPodMetricsInputConfigFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField("namespace").
			Description("The namespace for the Redpanda pod").
			Example([]string{"redpanda"}),
		service.NewStringField("pod_prefix").
			Description("The prefix of the Redpanda pod").
			Example([]string{"redpanda-0"}),
		service.NewStringField("pod_name").
			Description("The name of the pod").
			Example([]string{"redpanda-0"}),
		service.NewStringField("container").
			Description("The name of the container within the pod").
			Example([]string{"redpanda"}),
		service.NewTLSToggledField("tls"),
		kafka.SASLFields(),
		service.NewStringField("metrics_endpoint").Optional().Default("/metrics"),
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
	err := service.RegisterBatchInput("broker_pod_metrics", brokerPodMetricsInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			rdr, err := NewPodMetricsReaderFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatchedToggled(conf, rdr)
		})
	if err != nil {
		panic(err)
	}
}

// BrokerMetricsReader implements a metrics reader using the k8s-client library.
type BrokerMetricsReader struct {
	// to address the pod
	namespace string
	podPrefix string
	podName   string
	container string

	// to stream data
	url         string
	batchSize   int
	client      http.Client
	connMut     sync.Mutex
	connected   bool
	lastScraped time.Time
	eof         bool

	// for redpanda
	SeedBrokers     []string
	clientID        string
	TLSConf         *tls.Config
	saslConfs       []sasl.Mechanism
	nodeId          string
	clusterId       string
	metricsEndpoint string

	// for label handling
	showLabelPaths bool
	labelPaths     map[string]string
	labels         map[string]string

	// for management
	res     *service.Resources
	log     *service.Logger
	shutSig *shutdown.Signaller
}

// NewPodMetricsReaderFromConfig attempts to instantiate a new BrokerMetricsReader from a parsed config.
func NewPodMetricsReaderFromConfig(conf *service.ParsedConfig, res *service.Resources) (*BrokerMetricsReader, error) {
	r := BrokerMetricsReader{
		res:         res,
		log:         res.Logger(),
		shutSig:     shutdown.NewSignaller(),
		lastScraped: time.Now().Add(time.Duration(-1) * time.Hour),
	}

	var err error

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

	r.metricsEndpoint, err = conf.FieldString("metrics_endpoint")
	if err != nil {
		return nil, err
	}

	//r.closing = false

	return &r, nil
}

func (r *BrokerMetricsReader) populateRedpandaMetadata(ctx context.Context) error {
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

func (r *BrokerMetricsReader) getPodName() (string, error) {
	parts := strings.Split(r.podName, "-")
	ordinal := parts[len(parts)-1]
	return r.podPrefix + "-" + ordinal, nil
}

// Connect to the pod logs.
func (r *BrokerMetricsReader) Connect(ctx context.Context) error {
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

	cs, err := kubernetes.GetClientSet()
	maybePanic(err)

	//count := int64(10000)

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

	if r.TLSConf != nil {
		r.url = "https://" + host + ":" + adminPort + r.metricsEndpoint
	} else {
		r.url = "http://" + host + ":" + adminPort + r.metricsEndpoint
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

	if err != nil {
		return err
	}

	r.eof = false

	return nil
}

func (r *BrokerMetricsReader) enrichLine(line string) string {
	var sb strings.Builder
	leftBraceIndex := strings.Index(line, "{")
	if leftBraceIndex > 0 {
		sb.WriteString(line[:leftBraceIndex])
		sb.WriteString("{")
		first := true
		for k, v := range r.labels {
			if !first {
				sb.WriteString(",")
			}
			sb.WriteString(k)
			sb.WriteString("=\"")
			sb.WriteString(v)
			sb.WriteString("\"")
			first = false
		}
		if line[leftBraceIndex+1] == '}' {
			sb.WriteString(line[leftBraceIndex+1:])
		} else {
			sb.WriteString(",")
			sb.WriteString(line[leftBraceIndex+1:])
		}
	} else {
		sb.WriteString(line)
	}
	return sb.String()
}

func (r *BrokerMetricsReader) enrichScrape(bytes []byte) []byte {
	//for k, v := range r.labels {
	//	message.MetaSetMut(k, v)
	//}

	content := string(bytes)
	lines := strings.Split(content, "\n")
	results := make([]string, 0)

	for _, line := range lines {
		if strings.HasPrefix(line, "# HELP") || strings.HasPrefix(line, "# TYPE") {
			results = append(results, line)
		} else {
			enriched := r.enrichLine(line)
			results = append(results, enriched)
		}
	}

	return []byte(strings.Join(results, "\n"))
}

var tooSoon = errors.New("scrape too soon")

func (r *BrokerMetricsReader) doMetricsRequest(ctx context.Context) ([]byte, error) {
	if time.Now().Add(time.Duration(-30) * time.Second).After(r.lastScraped) {

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to construct request: %s", err)
		}

		resp, err := r.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to execute request: %s", err)
		}
		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				panic(err)
			}
		}(resp.Body)

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("request returned status: %d", resp.StatusCode)
		}

		var body []byte
		if body, err = io.ReadAll(resp.Body); err != nil {
			return nil, fmt.Errorf("failed to read response body: %s", err)
		}

		r.lastScraped = time.Now()

		return body, nil
	} else {
		return []byte{}, tooSoon
	}
}

func (r *BrokerMetricsReader) getMessages(ctx context.Context) (service.MessageBatch, error) {
	messages := service.MessageBatch{}

	bytes, err := r.doMetricsRequest(ctx)
	if errors.Is(err, tooSoon) {
		return messages, nil
	}
	if err != nil {
		return messages, err
	}

	content := string(bytes)
	lines := strings.Split(content, "\n")
	for _, line := range lines {
		message := service.NewMessage(r.enrichScrape([]byte(line)))
		message.MetaSetMut("timestamp", fmt.Sprintf("%v", time.Now().UnixNano()/1e6))
		messages = append(messages, message)
	}

	return messages, nil
}

// ReadBatch attempts to read a batch of log messages from the target pod container.
func (r *BrokerMetricsReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	//messages := service.MessageBatch{}
	//
	//bytes, err := r.doMetricsRequest(ctx)
	//if errors.Is(err, tooSoon) {
	//	return messages, func(ctx context.Context, res error) error {
	//		return nil
	//	}, nil
	//}
	//if err != nil {
	//	panic(err)
	//}
	//
	//message := service.NewMessage(r.enrichScrape(bytes))
	//messages = append(messages, message)

	messages, err := r.getMessages(ctx)
	if errors.Is(err, tooSoon) {
		return messages, func(ctx context.Context, res error) error {
			return nil
		}, nil
	}

	if len(messages) == 0 && r.eof == true {
		return nil, nil, service.ErrNotConnected
	}

	return messages, func(ctx context.Context, res error) error {
		return nil
	}, nil
}

// Close underlying connections.
func (r *BrokerMetricsReader) Close(ctx context.Context) error {
	//r.closing = true
	return nil
}
