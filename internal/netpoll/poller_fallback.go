//go:build !linux && !darwin && !freebsd && !openbsd

package netpoll

import "errors"

type fallbackPoller struct {
	cfg Config
}

func New(cfg Config) (Poller, error) {
	return nil, errors.New("netpoll not supported on this platform")
}

func (p *fallbackPoller) Add(fd int, op PollOp, userData uintptr) error {
	return errors.New("not supported")
}

func (p *fallbackPoller) Mod(fd int, op PollOp) error {
	return errors.New("not supported")
}

func (p *fallbackPoller) Del(fd int) error {
	return errors.New("not supported")
}

func (p *fallbackPoller) Wait(batch []Event) (n int, err error) {
	return 0, errors.New("not supported")
}

func (p *fallbackPoller) Close() error {
	return nil
}
