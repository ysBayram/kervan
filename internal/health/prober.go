package health

import (
	"context"
	"time"
)

type ProbeType int

const (
	ProbeTCP ProbeType = iota
	ProbeHTTP
	ProbeWebSocket
)

type ProbeConfig struct {
	Type     ProbeType
	HTTPPath string
	Timeout  time.Duration
}

type Prober interface {
	Probe(ctx context.Context, addr string) error
}
