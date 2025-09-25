package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/creasty/defaults"
	"gopkg.in/yaml.v3"
)

type Config struct {
	DumpBaseURL string `yaml:"dump_base_url" default:"https://dumps.wikimedia.org/kiwix/zim/wikipedia"`
	KafkaBroker string `yaml:"kafka_broker" default:"localhost:9092"`
	KafkaTopic  string `yaml:"kafka_topic" default:"wikipedia-pages"`
	WorkDir     string `yaml:"work_dir" default:"./data"`
	HTTPAddr    string `yaml:"http_addr" default:":8080"`
	Sink        string `yaml:"sink" default:"kafka"`
	StatusFile  string `yaml:"status_file" default:"status.log"`
}

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := defaults.Set(&cfg); err != nil {
		return Config{}, fmt.Errorf("set default config values: %w", err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	switch cfg.Sink {
	case "kafka":
	case "stdout":
		cfg.KafkaBroker = ""
		cfg.KafkaTopic = ""
	default:
		return Config{}, fmt.Errorf("unknown sink %q", cfg.Sink)
	}
	if cfg.StatusFile != "" && !filepath.IsAbs(cfg.StatusFile) {
		cfg.StatusFile = filepath.Join(cfg.WorkDir, cfg.StatusFile)
	}
	return cfg, nil
}
