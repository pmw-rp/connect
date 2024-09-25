package mimir

import (
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/redpanda-data/benthos/v4/public/service"
	"strconv"
	"strings"
	"time"
)

func mimirProcessorConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Categories("Integration").
		Summary("Processor to transform OpenTelemetry metrics into Mimir input (when combined with protobuf and snappy processors)").
		Description(`
This processor reads OpenTelemetry style metrics output and transforms it into Mimir-compatible records.
`)

	return spec
}

func newMimirProcessorFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*mimirProcessor, error) {
	return newMimirProcessor()
}

func newMimirProcessor() (*mimirProcessor, error) {
	s := &mimirProcessor{}
	return s, nil
}

func init() {
	err := service.RegisterBatchProcessor(
		"mimir_processor", mimirProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newMimirProcessorFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type mimirProcessor struct{}

type Datum struct {
	Kind   string `json:"type"`
	Metric string `json:"metricFamilyName"`
	Help   string `json:"help"`
	Unit   string `json:"unit"`
}

type Label struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Sample struct {
	TimestampMs string  `json:"timestampMs"`
	Value       float64 `json:"value"`
}

type Timeseries struct {
	Labels  []Label  `json:"labels"`
	Samples []Sample `json:"samples"`
}

type Result struct {
	Timeseries []Timeseries `json:"timeseries"`
	Metadata   []Datum      `json:"metadata"`
}

func processHelp(line string, metadata map[string]*Datum) {
	line = line[7:]
	firstSpace := strings.Index(line, " ")
	metric := line[:firstSpace]
	help := line[firstSpace+1:]
	d, ok := metadata[metric]
	if ok {
		d.Metric = metric
		d.Help = help
	} else {
		d := Datum{Metric: metric, Help: help}
		metadata[metric] = &d
	}
}

func processType(line string, metadata map[string]*Datum) {
	line = line[7:]
	firstSpace := strings.Index(line, " ")
	metric := line[:firstSpace]
	kind := line[firstSpace+1:]
	d, ok := metadata[metric]
	if ok {
		d.Metric = metric
		d.Kind = strings.ToUpper(kind)
	} else {
		d := Datum{Metric: metric, Kind: strings.ToUpper(kind)}
		metadata[metric] = &d
	}
}

func processLabels(line string) []Label {
	result := make([]Label, 0)
	if len(line) == 0 {
		return result
	}
	labels := strings.Split(line, ",")
	for _, l := range labels {
		pair := strings.Split(l, "=")
		name := pair[0]
		value := strings.Split(pair[1], "\"")[1]
		result = append(result, Label{Name: encode(name), Value: encode(value)})
	}
	return result
}

func processMetric(line string, timestampMs string) Timeseries {
	leftBrace := strings.Index(line, "{")
	metric := line[:leftBrace]
	rightBrace := strings.Index(line, "}")
	labelsString := line[leftBrace+1 : rightBrace]
	labels := processLabels(labelsString)
	labels = append(labels, Label{Name: encode("__name__"), Value: encode(metric)})
	value := line[rightBrace+2:]
	i, err := strconv.ParseFloat(value, 64)
	if err != nil {
		panic(err)
	}

	smp := Sample{TimestampMs: timestampMs, Value: i}
	samples := make([]Sample, 0)
	samples = append(samples, smp)

	return Timeseries{Labels: labels, Samples: samples}
}

func encode(line string) string {
	return b64.StdEncoding.EncodeToString([]byte(line))
}

// Process ProcessMessage
func (s *mimirProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {

	metadata := make(map[string]*Datum)
	metrics := make([]Timeseries, 0)

	for _, msg := range batch {

		bytes, err := msg.AsBytes()
		if err != nil {
			return nil, err
		}
		scrape := string(bytes)

		lines := strings.Split(scrape, "\n")
		for _, line := range lines {
			if len(line) > 0 {
				if strings.HasPrefix(line, "# HELP") {
					processHelp(line, metadata)
				} else if strings.HasPrefix(line, "# TYPE") {
					processType(line, metadata)
				} else if len(line) > 0 {
					ts, found := msg.MetaGet("timestamp")
					if !found {
						ts = fmt.Sprintf("%v", time.Now().UnixNano()/1e6)
					}
					timeseries := processMetric(line, ts)
					metrics = append(metrics, timeseries)
				}
			}
		}

	}

	meta := make([]Datum, 0)
	for _, d := range metadata {
		meta = append(meta, *d)
	}

	r := Result{Metadata: meta, Timeseries: metrics}

	j, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}

	msg := service.NewMessage(j)
	resultBatch := service.MessageBatch{msg}

	return []service.MessageBatch{resultBatch}, nil
}

// Close does nothing.
func (s *mimirProcessor) Close(ctx context.Context) error {
	return nil
}
