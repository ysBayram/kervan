package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type CoordinationConfig struct {
	Backend              string        `yaml:"backend"`
	LeaseTTL             time.Duration `yaml:"lease_ttl"`
	LeaseRefreshEvery    time.Duration `yaml:"lease_refresh_every"`
	DegradeProbeInterval time.Duration `yaml:"degrade_probe_interval"`
	DegradeThreshold     int           `yaml:"degrade_threshold"`
}

type Config struct {
	NodeID       string             `yaml:"node_id"`
	Listeners    []ListenerConfig   `yaml:"listeners"`
	Upstream     UpstreamConfig     `yaml:"upstream"`
	Session      SessionConfig      `yaml:"session"`
	Buffer       BufferConfig       `yaml:"buffer"`
	Failover     FailoverConfig     `yaml:"failover"`
	Coordination CoordinationConfig `yaml:"coordination"`
	MetricsAddr  string             `yaml:"metrics_addr"`
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

type FailoverConfig struct {
	ProbeInterval      time.Duration `yaml:"probe_interval"`
	UnhealthyThreshold uint32        `yaml:"unhealthy_threshold"`
	HealthyThreshold   uint32        `yaml:"healthy_threshold"`
	MaxFreezeDuration  time.Duration `yaml:"max_freeze_duration"`
	DrainTimeout       time.Duration `yaml:"drain_timeout"`
	ErrorRateWindow    time.Duration `yaml:"error_rate_window"`
	ErrorRateThreshold float64       `yaml:"error_rate_threshold"`
}

type BufferConfig struct {
	MaxEntries         uint32        `yaml:"max_entries"`
	MaxBytes           uint64        `yaml:"max_bytes"`
	InlineThreshold    int           `yaml:"inline_threshold"`
	MaxFrameSize       int           `yaml:"max_frame_size"`
	BackpressurePolicy string        `yaml:"backpressure_policy"`
	BlockTimeout       time.Duration `yaml:"block_timeout"`
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
