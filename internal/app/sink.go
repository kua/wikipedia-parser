package app

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"

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
		writer := &kafka.Writer{
			Addr:                   kafka.TCP(cfg.KafkaBroker),
			Topic:                  cfg.KafkaTopic,
			AllowAutoTopicCreation: false,
			BatchSize:              1,
			RequiredAcks:           kafka.RequireAll,
		}
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
