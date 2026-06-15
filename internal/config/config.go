package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	NodeID      string           `yaml:"node_id"`
	Listeners   []ListenerConfig `yaml:"listeners"`
	Upstream    UpstreamConfig   `yaml:"upstream"`
	Session     SessionConfig    `yaml:"session"`
	MetricsAddr string           `yaml:"metrics_addr"`
}

type ListenerConfig struct {
	Bind        string `yaml:"bind"`
	Protocol    string `yaml:"protocol"`
	MaxSessions int    `yaml:"max_sessions"`
}

type UpstreamConfig struct {
	Addr string `yaml:"addr"`
}

type SessionConfig struct {
	ShardCount     uint32        `yaml:"shard_count"`
	IdleTimeout    time.Duration `yaml:"idle_timeout"`
	ReadBufferSize int           `yaml:"read_buffer_size"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
