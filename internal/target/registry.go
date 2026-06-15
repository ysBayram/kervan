package target

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ysBayram/kervan/internal/coordination"
	"github.com/ysBayram/kervan/internal/health"
	"github.com/ysBayram/kervan/internal/session"
)

type RegistryConfig struct {
	ProbeInterval      time.Duration
	UnhealthyThreshold uint32
	HealthyThreshold   uint32
	MaxFreezeDuration  time.Duration
	DrainTimeout       time.Duration
	ErrorRateWindow    time.Duration
	ErrorRateThreshold float64
}

type TargetMetrics struct {
	SessionsFrozen atomic.Int64
	FailoverTotal  atomic.Int64
}

type Registry struct {
	cfg       RegistryConfig
	targets   sync.Map
	router    *Router
	sessions  *session.SessionManager
	store     coordination.CoordinationStore
	prober    health.Prober
	passive   *health.PassiveObserver
	metrics   *TargetMetrics
	nodeID    string
	stopCh    chan struct{}
}

func NewRegistry(cfg RegistryConfig, deps struct {
	Router   *Router
	Sessions *session.SessionManager
	Store    coordination.CoordinationStore
	Prober   health.Prober
	NodeID   string
}) *Registry {
	return &Registry{
		cfg:      cfg,
		router:   deps.Router,
		sessions: deps.Sessions,
		store:    deps.Store,
		prober:   deps.Prober,
		passive:  health.NewPassiveObserver(cfg.ErrorRateWindow),
		metrics:  &TargetMetrics{},
		nodeID:   deps.NodeID,
		stopCh:   make(chan struct{}),
	}
}

func (r *Registry) Run(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.ProbeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.probeAll()
		}
	}
}

func (r *Registry) Stop() {
	close(r.stopCh)
}

func (r *Registry) probeAll() {
	r.targets.Range(func(key, val any) bool {
		t := val.(*Target)
		go r.probeTarget(t)
		return true
	})
}

func (r *Registry) probeTarget(t *Target) {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.ProbeInterval)
	defer cancel()

	err := r.prober.Probe(ctx, t.Addr)
	if err != nil {
		t.Failures.Add(1)
		t.Successes.Store(0)
		if t.Failures.Load() >= r.cfg.UnhealthyThreshold {
			r.OnTargetUnhealthy(t)
		}
	} else {
		t.Successes.Add(1)
		t.Failures.Store(0)
		if t.Successes.Load() >= r.cfg.HealthyThreshold {
			r.OnTargetHealthy(t)
		}
	}
	t.LastProbe.Store(time.Now().UnixNano())
}

func (r *Registry) AddTarget(t *Target) {
	r.targets.Store(t.ID, t)
	r.store.RegisterTarget(context.Background(), *t)
}

func (r *Registry) RemoveTarget(id TargetID) {
	r.targets.Delete(id)
}

func (r *Registry) GetTarget(id TargetID) (*Target, bool) {
	val, ok := r.targets.Load(id)
	if !ok {
		return nil, false
	}
	return val.(*Target), true
}

func (r *Registry) SelectTarget(route, clientID string) (*Target, error) {
	targetID, err := r.router.Select(route, clientID)
	if err != nil {
		return nil, err
	}
	return r.GetTarget(targetID)
}

func (r *Registry) SelectHealthyTarget(route, clientID string) (*Target, error) {
	targetID, err := r.router.SelectHealthy(route, clientID, func(id TargetID) bool {
		t, ok := r.GetTarget(id)
		return ok && t.GetState() == TargetHealthy
	})
	if err != nil {
		return nil, err
	}
	return r.GetTarget(targetID)
}

func (r *Registry) NotifyWriteError(targetID TargetID, err error) {
	rate := r.passive.RecordWrite(string(targetID), err)
	if rate >= r.cfg.ErrorRateThreshold {
		if t, ok := r.GetTarget(targetID); ok {
			r.triggerFreeze(t)
		}
	}
}

func (r *Registry) triggerFreeze(t *Target) {
	t.State.Store(uint32(TargetUnhealthy))
	r.OnTargetUnhealthy(t)
}

func (r *Registry) OnTargetUnhealthy(t *Target) {
	// Stub: FreezeController.FreezeTarget will be called by external orchestrator
	r.store.PublishTargetHealth(context.Background(), t.ID, TargetUnhealthy)
}

func (r *Registry) OnTargetHealthy(t *Target) {
	r.store.PublishTargetHealth(context.Background(), t.ID, TargetHealthy)
}
