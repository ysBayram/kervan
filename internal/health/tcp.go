package health

import (
	"context"
	"net"
	"time"
)

type TCPProber struct {
	timeout time.Duration
}

func NewTCPProber(timeout time.Duration) *TCPProber {
	return &TCPProber{timeout: timeout}
}

func (p *TCPProber) Probe(ctx context.Context, addr string) error {
	dialer := net.Dialer{Timeout: p.timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}
