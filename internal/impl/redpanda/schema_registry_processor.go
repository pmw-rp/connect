package redpanda

import (
	"context"
	"cuelang.org/go/pkg/strconv"
	"encoding/json"
	"errors"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func schemaRegistryProcessorConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Categories("Integration").
		Summary("Filters invalid messages from Redpanda schemas topic").
		Description(`
This processor filters messages read from a Redpanda _schemas topic where the key defines a sequence number, but the
sequence number doesn't match the offset of the message that holds it.

For any message that is considered valid (the sequence number does exist and does match the offset), the message is
emitted (but without the sequence number or the node id).
`)

	return spec
}

func newSchemaRegistryProcessorFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*schemaRegistryProcessor, error) {
	return newSchemaRegistryProcessor()
}

func newSchemaRegistryProcessor() (*schemaRegistryProcessor, error) {
	s := &schemaRegistryProcessor{}
	return s, nil
}

func init() {
	err := service.RegisterProcessor(
		"schema_registry_processor", schemaRegistryProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newSchemaRegistryProcessorFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type schemaRegistryProcessor struct{}

func ExtractKey(msg *service.Message) (map[string]any, error) {
	k, found := msg.MetaGet("kafka_key")
	if !found {
		return nil, errors.New("can't process a schema message without a key")
	}
	var key map[string]any
	err := json.Unmarshal([]byte(k), &key)
	return key, err
}

func getOffset(msg *service.Message) (int64, error) {
	o, found := msg.MetaGet("kafka_offset")
	if !found {
		return 0, errors.New("benthos message doesn't contain kafka_offset as expected")
	}
	offset, err := strconv.ParseInt(o, 10, 64)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

// Process ProcessMessage
func (s *schemaRegistryProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {

	key, err := ExtractKey(msg)
	if err != nil {
		return []*service.Message{msg}, nil
	}
	if _, found := key["seq"]; !found {
		return []*service.Message{msg}, nil
	}

	seq := int64(key["seq"].(float64))

	offset, err := getOffset(msg)
	if err != nil {
		return nil, err
	}

	value, err := msg.AsBytes()
	if err != nil {
		return []*service.Message{msg}, err
	}
	if seq != offset && len(value) > 0 {
		return []*service.Message{}, nil
	}

	delete(key, "seq")
	delete(key, "node")

	bytes, err := json.Marshal(key)
	if err != nil {
		return []*service.Message{msg}, err
	}

	//msg = msg.DeepCopy()
	msg.MetaSetMut("kafka_key", string(bytes))

	return []*service.Message{msg}, nil
}

// Close does nothing.
func (s *schemaRegistryProcessor) Close(ctx context.Context) error {
	return nil
}
