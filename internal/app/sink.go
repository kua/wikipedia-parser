package app

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	kafkacompress "github.com/segmentio/kafka-go/compress"

	"github.com/example/wikipedia-parser/internal/config"
	"github.com/example/wikipedia-parser/internal/exporter"
	"github.com/example/wikipedia-parser/internal/pageproto"
	"google.golang.org/protobuf/encoding/protojson"
)

func BuildSink(cfg config.Config) (exporter.Sink, func(), error) {
	switch cfg.Sink {
	case "stdout":
		return stdoutSink{}, func() {}, nil
	case "kafka":
		brokers := cfg.KafkaBroker
		if len(brokers) == 0 {
			return nil, nil, fmt.Errorf("kafka brokers are not configured")
		}
		topic := cfg.KafkaTopic
		writer := kafka.NewWriter(kafka.WriterConfig{
			Brokers:                brokers,
			Topic:                  topic,
			AllowAutoTopicCreation: false,
			BatchSize:              100000,
			BatchBytes:             100000000,
			BatchTimeout:           time.Second,
			RequiredAcks:           kafka.RequireAll,
			CompressionCodec:       &kafkacompress.SnappyCodec,
			MaxAttempts:            11,
			QueueCapacity:          10000000,
		})
		writer.AllowAutoTopicCreation = false
		return kafkaSink{writer: writer}, func() { writer.Close() }, nil
	default:
		return nil, nil, fmt.Errorf("unknown sink %q", cfg.Sink)
	}
}

type kafkaSink struct {
	writer *kafka.Writer
}

func (k kafkaSink) Send(ctx context.Context, key, value []byte) error {
	return k.writer.WriteMessages(ctx, kafka.Message{Key: key, Value: value})
}

type stdoutSink struct{}

func (stdoutSink) Send(ctx context.Context, key, value []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	page, err := pageproto.Unmarshal(value)
	if err != nil {
		fmt.Printf("stdout sink %s (%d bytes) decode error: %v\n", string(key), len(value), err)
		return nil
	}
	opts := protojson.MarshalOptions{UseProtoNames: true, Multiline: true, Indent: "  "}
	data, err := opts.Marshal(page)
	if err != nil {
		fmt.Printf("stdout sink %s (%d bytes) json encode error: %v\n", string(key), len(value), err)
		return nil
	}
	fmt.Printf("stdout sink %s %s:\n%s\n", string(key), data, string(page.Content))
	return nil
}
