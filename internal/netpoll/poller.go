package netpoll

import (
	"errors"
	"sync/atomic"
)

type PollOp uint8

const (
	PollRead PollOp = 1 << iota
	PollWrite
	PollReadWrite = PollRead | PollWrite
)

type Event struct {
	FD       int32
	Op       PollOp
	Flags    uint32
	UserData uintptr
}

type Handler func(ev Event) error

type Poller interface {
	Add(fd int, op PollOp, userData uintptr) error
	Mod(fd int, op PollOp) error
	Del(fd int) error
	Wait(batch []Event) (n int, err error)
	Close() error
}

type Config struct {
	BatchSize int
}

var ErrPollClosed = errors.New("poller closed")

type FDRegistry struct {
	slots []atomic.Pointer[any]
}

func NewFDRegistry(maxFD int) *FDRegistry {
	return &FDRegistry{slots: make([]atomic.Pointer[any], maxFD)}
}

func (r *FDRegistry) Register(fd int, data any) {
	if fd >= 0 && fd < len(r.slots) {
		r.slots[fd].Store(&data)
	}
}

func (r *FDRegistry) Lookup(fd int) any {
	if fd >= 0 && fd < len(r.slots) {
		ptr := r.slots[fd].Load()
		if ptr != nil {
			return *ptr
		}
	}
	return nil
}

func (r *FDRegistry) Unregister(fd int) {
	if fd >= 0 && fd < len(r.slots) {
		r.slots[fd].Store(nil)
	}
}
