package health

import (
	"sync"
	"sync/atomic"
	"time"
)

type errorWindow struct {
	total   atomic.Uint64
	errors  atomic.Uint64
	resetAt atomic.Int64
}

type PassiveObserver struct {
	windows sync.Map
	window  time.Duration
}

func NewPassiveObserver(window time.Duration) *PassiveObserver {
	return &PassiveObserver{window: window}
}

func (p *PassiveObserver) RecordWrite(targetID string, err error) float64 {
	val, _ := p.windows.LoadOrStore(targetID, &errorWindow{
		resetAt: atomic.Int64{},
	})
	w := val.(*errorWindow)

	now := time.Now().UnixNano()
	reset := w.resetAt.Load()
	if now > reset {
		w.total.Store(0)
		w.errors.Store(0)
		w.resetAt.Store(now + p.window.Nanoseconds())
	}

	w.total.Add(1)
	if err != nil {
		w.errors.Add(1)
	}

	total := w.total.Load()
	errors := w.errors.Load()
	if total == 0 {
		return 0
	}
	return float64(errors) / float64(total)
}
