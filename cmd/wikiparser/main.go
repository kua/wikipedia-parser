package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/segmentio/kafka-go"

	"github.com/example/wikipedia-parser/internal/config"
	"github.com/example/wikipedia-parser/internal/exporter"
	"github.com/example/wikipedia-parser/internal/pageproto"
)

func main() {
	cfgPath := flag.String("config", "config.yml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	if err := os.MkdirAll(cfg.WorkDir, 0o755); err != nil {
		log.Fatal(err)
	}
	client := &http.Client{}

	sink, closer := buildSink(cfg)
	defer closer()

	svc := exporter.NewService(cfg, client, sink)

	mux := http.NewServeMux()
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		if err := svc.Start(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("/pause", func(w http.ResponseWriter, r *http.Request) {
		if err := svc.Pause(); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("/abort", func(w http.ResponseWriter, r *http.Request) {
		if err := svc.Abort(); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	mux.Handle("/metrics", svc.MetricsHandler())
	mux.HandleFunc("/list", func(w http.ResponseWriter, r *http.Request) {
		list := svc.List()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(list)
	})
	mux.HandleFunc("/list/all", func(w http.ResponseWriter, r *http.Request) {
		result, err := svc.ListAll(r.Context())
		status := http.StatusOK
		if err != nil {
			status = http.StatusBadGateway
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(result)
	})

	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: mux,
	}
	log.Printf("listening on %s", cfg.HTTPAddr)
	log.Fatal(server.ListenAndServe())
}

type sinkCloser func()

func buildSink(cfg config.Config) (exporter.Sink, sinkCloser) {
	switch cfg.Sink {
	case "stdout":
		return stdoutSink{}, func() {}
	default:
		writer := &kafka.Writer{
			Addr:                   kafka.TCP(cfg.KafkaBroker),
			Topic:                  cfg.KafkaTopic,
			AllowAutoTopicCreation: false,
			BatchSize:              1,
			RequiredAcks:           kafka.RequireAll,
		}
		return kafkaSink{writer: writer}, func() { writer.Close() }
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
	payload := struct {
		SrcURL   string            `json:"src_url"`
		HTTPCode uint32            `json:"http_code"`
		Headers  map[string]string `json:"headers,omitempty"`
		Content  string            `json:"content"`
		IP       uint32            `json:"ip"`
	}{
		SrcURL:   page.SrcURL,
		HTTPCode: page.HTTPCode,
		Headers:  page.Headers,
		Content:  string(page.Content),
		IP:       page.IP,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		fmt.Printf("stdout sink %s (%d bytes) json encode error: %v\n", string(key), len(value), err)
		return nil
	}
	fmt.Printf("stdout sink %s:\n%s\n", string(key), data)
	return nil
}
