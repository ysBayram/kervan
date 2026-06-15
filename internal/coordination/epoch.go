package coordination

import (
	"context"
	"sync/atomic"
)

type EpochWatcher struct {
	current  atomic.Uint64
	onChange func(newEpoch uint64)
	stopCh   chan struct{}
}

func NewEpochWatcher(onChange func(uint64)) *EpochWatcher {
	return &EpochWatcher{
		onChange: onChange,
		stopCh:   make(chan struct{}),
	}
}

func (ew *EpochWatcher) Current() uint64 {
	return ew.current.Load()
}

func (ew *EpochWatcher) Increment() uint64 {
	next := ew.current.Add(1)
	if ew.onChange != nil {
		ew.onChange(next)
	}
	return next
}

func (ew *EpochWatcher) Run(ctx context.Context) {
	<-ctx.Done()
}
