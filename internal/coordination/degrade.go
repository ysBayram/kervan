package coordination

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type DegradeController struct {
	store     CoordinationStore
	degraded  atomic.Bool
	lastKnown sync.Map
	stopCh    chan struct{}
}

func NewDegradeController(store CoordinationStore) *DegradeController {
	return &DegradeController{
		store:  store,
		stopCh: make(chan struct{}),
	}
}

func (dc *DegradeController) IsDegraded() bool {
	return dc.degraded.Load()
}

func (dc *DegradeController) EnterDegraded() {
	dc.degraded.Store(true)
}

func (dc *DegradeController) ExitDegraded() {
	dc.degraded.Store(false)
}

func (dc *DegradeController) Run(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var failures int
	for {
		select {
		case <-ctx.Done():
			return
		case <-dc.stopCh:
			return
		case <-ticker.C:
			failures++
			if failures >= 3 {
				dc.EnterDegraded()
			}
		}
	}
}
