package redpanda

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func newMessage(key string, value string, offset int32) *service.Message {
	msg := service.NewMessage([]byte(value))
	msg.MetaSetMut("kafka_key", []byte(key))
	msg.MetaSetMut("kafka_offset", offset)
	return msg
}

func TestSchemaRegistryProcessor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	proc, err := newSchemaRegistryProcessor()
	require.NoError(t, err, "failed to create processor")
	t.Cleanup(func() { require.NoError(t, proc.Close(ctx), "failed to close processor") })

	{ // Message doesn't contain JSON
		msg := newMessage("foo", "bar", 0)
		b, err := proc.Process(ctx, msg)
		require.NoError(t, err, "failed to processor message")
		require.Len(t, b, 1, "wrong batch size received")
		require.Same(t, msg, b[0])
	}

	{ // Message contains JSON without seq
		msg := newMessage("{\n  \"keytype\": \"SCHEMA\",\n  \"magic\": 1,\n  \"subject\": \"nasdaq_historical_proto-value\",\n  \"version\": 1\n}", "{\n  \"subject\": \"nasdaq_historical_proto-value\",\n  \"version\": 1,\n  \"id\": 1,\n  \"schemaType\": \"PROTOBUF\",\n  \"schema\": \"syntax = \\\"proto3\\\"; package nasdaq.historical.v1; message NasdaqHistorical { string date = 1; string last = 2; float volume = 3; string open = 4; string high = 5; string low = 6; } \",\n  \"deleted\": false\n}", 0)
		b, err := proc.Process(ctx, msg)
		require.NoError(t, err, "failed to processor message")
		require.Len(t, b, 1, "wrong batch size received")
		require.Same(t, msg, b[0])
	}

	{ // Message contains JSON with matching seq
		reference := newMessage("{\n  \"keytype\": \"SCHEMA\",\n  \"magic\": 1,\n  \"subject\": \"nasdaq_historical_proto-value\",\n  \"version\": 1,\n  \"node\": 0,\n  \"seq\": 0\n}", "{\n  \"subject\": \"nasdaq_historical_proto-value\",\n  \"version\": 1,\n  \"id\": 1,\n  \"schemaType\": \"PROTOBUF\",\n  \"schema\": \"syntax = \\\"proto3\\\"; package nasdaq.historical.v1; message NasdaqHistorical { string date = 1; string last = 2; float volume = 3; string open = 4; string high = 5; string low = 6; } \",\n  \"deleted\": false\n}", 0)
		input := newMessage("{\n  \"keytype\": \"SCHEMA\",\n  \"magic\": 1,\n  \"subject\": \"nasdaq_historical_proto-value\",\n  \"version\": 1,\n  \"node\": 0,\n  \"seq\": 0\n}", "{\n  \"subject\": \"nasdaq_historical_proto-value\",\n  \"version\": 1,\n  \"id\": 1,\n  \"schemaType\": \"PROTOBUF\",\n  \"schema\": \"syntax = \\\"proto3\\\"; package nasdaq.historical.v1; message NasdaqHistorical { string date = 1; string last = 2; float volume = 3; string open = 4; string high = 5; string low = 6; } \",\n  \"deleted\": false\n}", 0)

		results, err := proc.Process(ctx, input)
		require.NoError(t, err, "failed to processor message")
		require.Len(t, results, 1, "wrong batch size received")
		require.Same(t, input, results[0])

		referenceBytes, err := reference.AsBytes()
		resultsBytes, err := results[0].AsBytes()
		require.Equal(t, referenceBytes, resultsBytes)

		referenceKey, err := ExtractKey(reference)
		resultKey, err := ExtractKey(results[0])

		require.Equal(t, 6, len(referenceKey))
		require.Equal(t, 4, len(resultKey))
	}

	{ // Message contains JSON with non-matching seq and value is non-empty
		msg := newMessage("{\n  \"keytype\": \"SCHEMA\",\n  \"magic\": 1,\n  \"subject\": \"nasdaq_historical_proto-value\",\n  \"version\": 1,\n  \"node\": 0,\n  \"seq\": 0\n}", "{\n  \"subject\": \"nasdaq_historical_proto-value\",\n  \"version\": 1,\n  \"id\": 1,\n  \"schemaType\": \"PROTOBUF\",\n  \"schema\": \"syntax = \\\"proto3\\\"; package nasdaq.historical.v1; message NasdaqHistorical { string date = 1; string last = 2; float volume = 3; string open = 4; string high = 5; string low = 6; } \",\n  \"deleted\": false\n}", 1)
		b, err := proc.Process(ctx, msg)
		require.NoError(t, err, "failed to processor message")
		require.Len(t, b, 0, "wrong batch size received")
	}

	{ // Message contains JSON with non-matching seq and value is empty (deletion)
		reference := newMessage("{\n  \"keytype\": \"SCHEMA\",\n  \"magic\": 1,\n  \"subject\": \"nasdaq_historical_proto-value\",\n  \"version\": 1,\n  \"node\": 0,\n  \"seq\": 0\n}", "", 1)
		input := newMessage("{\n  \"keytype\": \"SCHEMA\",\n  \"magic\": 1,\n  \"subject\": \"nasdaq_historical_proto-value\",\n  \"version\": 1,\n  \"node\": 0,\n  \"seq\": 0\n}", "", 1)
		results, err := proc.Process(ctx, input)
		require.NoError(t, err, "failed to processor message")
		require.Len(t, results, 1, "wrong batch size received")
		require.Same(t, input, results[0])

		referenceBytes, err := reference.AsBytes()
		resultsBytes, err := results[0].AsBytes()
		require.Equal(t, referenceBytes, resultsBytes)

		referenceKey, err := ExtractKey(reference)
		resultKey, err := ExtractKey(results[0])

		require.Equal(t, 6, len(referenceKey))
		require.Equal(t, 4, len(resultKey))
	}

}
