package loki

import (
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"github.com/redpanda-data/benthos/v4/public/service"
	"regexp"
	"sort"
	"strings"
	"time"
)

func lokiProcessorConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Categories("Integration").
		Summary("Processor to transform OpenTelemetry metrics into Mimir input (when combined with protobuf and snappy processors)").
		Description(`
This processor reads OpenTelemetry style metrics output and transforms it into Mimir-compatible records.
`).Fields(LokiProcessorConfigFields()...)
	return spec
}

// LokiProcessorConfigFields returns the full suite of config fields
func LokiProcessorConfigFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewObjectField("timestamps",
			service.NewBoolField("enabled").Default(true).Description("Whether to use timestamps from the record or just use time.Now()"),
			service.NewStringField("selector").Default("timestamp").Description("The name of the metadata to interpret as the record timestamp"),
			service.NewStringField("format").Default("2006-01-02 15:04:05,000")),
		service.NewStringListField("labels"),
	}
}

func newLokiProcessorFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*lokiProcessor, error) {

	var f lokiProcessor

	// Timestamps
	if enabled, err := conf.FieldBool("timestamps", "enabled"); err == nil {
		f.timestampConf.enabled = enabled
		if f.timestampConf.enabled {
			if selector, err := conf.FieldString("timestamps", "selector"); err == nil {
				f.timestampConf.selector = selector
			} else {
				return nil, err
			}
			if format, err := conf.FieldString("timestamps", "format"); err == nil {
				f.timestampConf.format = format
			} else {
				return nil, err
			}
		}
	} else {
		return nil, err
	}

	// Labels
	f.labels, _ = conf.FieldStringList("labels")
	var regex strings.Builder
	for i, label := range f.labels {
		if i > 0 {
			regex.WriteString("|")
		}
		regex.WriteString("(^")
		regex.WriteString(label)
		regex.WriteString("$)")
	}
	compiledRegex, err := regexp.Compile(regex.String())
	if err != nil {
		return nil, err
	}

	f.labelsRegex = compiledRegex

	return &f, nil
}

func init() {
	err := service.RegisterBatchProcessor(
		"loki_processor", lokiProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newLokiProcessorFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type lokiProcessor struct {
	labels        []string
	labelsRegex   *regexp.Regexp
	timestampConf TimestampConf
}

type PushRequest struct {
	Streams []StreamAdapter `json:"streams"`
}

type StreamAdapter struct {
	Labels  string         `json:"labels"`
	Entries []EntryAdapter `json:"entries"`
	Hash    uint64         `json:"-"`
}

type EntryAdapter struct {
	Timestamp          time.Time          `json:"timestamp"`
	Line               string             `json:"line"`
	StructuredMetadata []LabelPairAdapter `json:"structuredMetadata,omitempty"`
	Parsed             []LabelPairAdapter `json:"parsed,omitempty"` // Do not use
}

type LabelPairAdapter struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func encode(line string) string {
	return b64.StdEncoding.EncodeToString([]byte(line))
}

// Process ProcessBatch
func (s *lokiProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {

	result := make([]service.MessageBatch, 0)
	var request PushRequest

	streams := make(map[string]*StreamAdapter)

	for _, message := range batch {

		structuredMetadata := make([]LabelPairAdapter, 0)
		parsed := make([]LabelPairAdapter, 0)

		bytes, err := message.AsBytes()

		if err != nil {
			panic(err)
		}

		line := string(bytes)

		labelMap := make(map[string]string)

		var timestamp time.Time

		err = message.MetaWalk(func(k string, v string) error {
			if s.labelsRegex.Match([]byte(k)) {
				labelMap[k] = v
			} else if s.timestampConf.enabled && k == s.timestampConf.selector {
				timestamp, err = time.Parse(s.timestampConf.format, v)
				if err != nil {
					return err
				}
			} else {
				structuredMetadata = append(structuredMetadata, LabelPairAdapter{
					Name:  k,
					Value: v,
				})
			}
			return nil
		})

		var labelsBuilder strings.Builder
		firstLabel := true

		keys := make([]string, 0)
		for k := range labelMap {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		labelsBuilder.WriteString("{")
		for _, k := range keys {
			if !firstLabel {
				labelsBuilder.WriteString(",")
			}
			labelsBuilder.WriteString(k)
			labelsBuilder.WriteString("=\"")
			labelsBuilder.WriteString(labelMap[k])
			labelsBuilder.WriteString("\"")
			firstLabel = false
		}
		labelsBuilder.WriteString("}")

		labels := labelsBuilder.String()

		if err != nil {
			return nil, err
		}

		if s.timestampConf.enabled == false {
			timestamp = time.Now()
		}

		stream, ok := streams[labels]
		if !ok {
			stream = &StreamAdapter{
				Labels:  labels,
				Entries: make([]EntryAdapter, 0),
				Hash:    0,
			}
			streams[labels] = stream
		}

		stream.Entries = append(stream.Entries, EntryAdapter{
			Timestamp:          timestamp,
			Line:               line,
			StructuredMetadata: structuredMetadata,
			Parsed:             parsed,
		})

	}

	streamsArray := make([]StreamAdapter, 0)
	for _, v := range streams {
		streamsArray = append(streamsArray, *v)
	}

	request.Streams = streamsArray

	j, err := json.Marshal(request)
	if err != nil {
		panic(err)
	}
	msg := service.NewMessage(j)
	result = append(result, service.MessageBatch{msg})
	//fmt.Println(string(j))
	return result, nil
}

// Close does nothing.
func (s *lokiProcessor) Close(ctx context.Context) error {
	return nil
}
