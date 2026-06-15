package coordination

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrLeaseHeldByOther = errors.New("lease held by another node")

type LeaseConfig struct {
	TTL          time.Duration
	RefreshEvery time.Duration
}

type sessionLease struct {
	mu       sync.Mutex
	clientID string
	nodeID   string
	expires  time.Time
	ctx      context.Context
	cancel   context.CancelFunc
}

type LeaseManager struct {
	store  CoordinationStore
	nodeID string
	cfg    LeaseConfig
	leases sync.Map
}

func NewLeaseManager(store CoordinationStore, nodeID string, cfg LeaseConfig) *LeaseManager {
	return &LeaseManager{
		store:  store,
		nodeID: nodeID,
		cfg:    cfg,
	}
}

func (lm *LeaseManager) Acquire(ctx context.Context, clientID string) (Lease, error) {
	lease := &sessionLease{
		clientID: clientID,
		nodeID:   lm.nodeID,
		expires:  time.Now().Add(lm.cfg.TTL),
	}
	lease.ctx, lease.cancel = context.WithCancel(ctx)

	lm.leases.Store(clientID, lease)
	go lm.refreshLoop(lease)
	return lease, nil
}

func (lm *LeaseManager) Refresh(ctx context.Context, l Lease) error {
	sl, ok := l.(*sessionLease)
	if !ok {
		return errors.New("invalid lease type")
	}
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.expires = time.Now().Add(lm.cfg.TTL)
	return nil
}

func (lm *LeaseManager) Release(l Lease) error {
	sl, ok := l.(*sessionLease)
	if !ok {
		return errors.New("invalid lease type")
	}
	lm.leases.Delete(sl.clientID)
	sl.cancel()
	return nil
}

func (lm *LeaseManager) refreshLoop(l *sessionLease) {
	ticker := time.NewTicker(lm.cfg.RefreshEvery)
	defer ticker.Stop()
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			lm.Refresh(l.ctx, l)
		}
	}
}
