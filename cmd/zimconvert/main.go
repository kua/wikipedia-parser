package main

import (
	"context"
	"flag"
	"log"

	"github.com/example/wikipedia-parser/internal/app"
	"github.com/example/wikipedia-parser/internal/config"
	"github.com/example/wikipedia-parser/internal/exporter"
)

func main() {
	cfgPath := flag.String("config", "config.yml", "path to config file")
	filePath := flag.String("file", "", "path to ZIM archive")
	flag.Parse()

	if *filePath == "" {
		log.Fatal("file path must be provided with -file")
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}

	sink, closer, err := app.BuildSink(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer closer()

	if err := exporter.ConvertFile(context.Background(), cfg, sink, *filePath); err != nil {
		log.Fatal(err)
	}
}
