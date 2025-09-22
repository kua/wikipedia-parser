package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	DumpBaseURL       string `yaml:"dump_base_url"`
	KafkaBroker       string `yaml:"kafka_broker"`
	KafkaTopic        string `yaml:"kafka_topic"`
	WorkDir           string `yaml:"work_dir"`
	HTTPAddr          string `yaml:"http_addr"`
	Sink              string `yaml:"sink"`
	StatusFile        string `yaml:"status_file"`
	MediaWikiAPI      string `yaml:"mediawiki_api_url"`
	MaxRenderInflight int    `yaml:"max_render_inflight"`
}

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	cfg := Config{HTTPAddr: ":8080", Sink: "kafka", MaxRenderInflight: 4}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	missing := make([]string, 0)
	if cfg.DumpBaseURL == "" {
		missing = append(missing, "dump_base_url")
	}
	if cfg.WorkDir == "" {
		missing = append(missing, "work_dir")
	}
	if cfg.MediaWikiAPI == "" {
		missing = append(missing, "mediawiki_api_url")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing %s", strings.Join(missing, ", "))
	}
	if cfg.Sink == "" {
		cfg.Sink = "kafka"
	}
	switch cfg.Sink {
	case "kafka":
		kafkaMissing := make([]string, 0)
		if cfg.KafkaBroker == "" {
			kafkaMissing = append(kafkaMissing, "kafka_broker")
		}
		if cfg.KafkaTopic == "" {
			kafkaMissing = append(kafkaMissing, "kafka_topic")
		}
		if len(kafkaMissing) > 0 {
			return Config{}, fmt.Errorf("missing %s", strings.Join(kafkaMissing, ", "))
		}
	case "stdout":
		cfg.KafkaBroker = ""
		cfg.KafkaTopic = ""
	default:
		return Config{}, fmt.Errorf("unknown sink %q", cfg.Sink)
	}
	if cfg.StatusFile == "" {
		cfg.StatusFile = filepath.Join(cfg.WorkDir, "status.log")
	}
	if cfg.MaxRenderInflight <= 0 {
		cfg.MaxRenderInflight = 1
	}
	return cfg, nil
}
