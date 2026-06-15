package target

import (
	"sync/atomic"
)

type TargetID string

type TargetState uint32

const (
	TargetUnknown  TargetState = 0
	TargetHealthy  TargetState = 1
	TargetUnhealthy TargetState = 2
)

type Target struct {
	ID         TargetID
	Addr       string
	Route      string
	Weight     int32
	State      atomic.Uint32
	Failures   atomic.Uint32
	Successes  atomic.Uint32
	LastProbe  atomic.Int64
}

func (t *Target) CASState(from, to TargetState) bool {
	return t.State.CompareAndSwap(uint32(from), uint32(to))
}

func (t *Target) GetState() TargetState {
	return TargetState(t.State.Load())
}
