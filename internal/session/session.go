package session

import (
	"sync/atomic"
	"time"
)

type State uint32

const (
	StateCreated State = iota
	StateActive
	StateFrozen
	StateDraining
	StateClosed
)

type Protocol uint8

const (
	ProtocolTCP Protocol = iota
	ProtocolWebSocket
)

type ClientSession struct {
	ClientID     string
	ClientFD     int
	UpstreamFD   int
	State        atomic.Uint32
	Protocol     Protocol
	TargetID     string
	Route        string
	ShardIdx     uint8
	CreatedAt    int64
	LastActivity atomic.Int64
	_            [64]byte
}

func (s *ClientSession) CASState(from, to State) bool {
	return s.State.CompareAndSwap(uint32(from), uint32(to))
}

func (s *ClientSession) GetState() State {
	return State(s.State.Load())
}

func (s *ClientSession) Touch() {
	s.LastActivity.Store(nowNano())
}

func nowNano() int64 {
	return time.Now().UnixNano()
}
