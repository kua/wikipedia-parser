package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/example/wikipedia-parser/internal/app"
	"github.com/example/wikipedia-parser/internal/config"
	"github.com/example/wikipedia-parser/internal/exporter"
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

	sink, closer, err := app.BuildSink(cfg)
	if err != nil {
		log.Fatal(err)
	}
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
