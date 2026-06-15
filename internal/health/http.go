package health

import (
	"context"
	"net/http"
	"time"
)

type HTTPProber struct {
	client *http.Client
	path   string
}

func NewHTTPProber(path string, timeout time.Duration) *HTTPProber {
	return &HTTPProber{
		client: &http.Client{Timeout: timeout},
		path:   path,
	}
}

func (p *HTTPProber) Probe(ctx context.Context, addr string) error {
	url := "http://" + addr + p.path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}
