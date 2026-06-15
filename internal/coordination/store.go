package coordination

import (
	"context"
	"time"

	"github.com/ysBayram/kervan/internal/target"
)

type EventType int

const (
	EventAdded EventType = iota
	EventRemoved
	EventHealthChanged
)

type TargetEvent struct {
	Target target.Target
	Type   EventType
}

type Lease interface {
	Refresh(ctx context.Context) error
	Release() error
	Key() string
}

type CoordinationStore interface {
	RegisterTarget(ctx context.Context, t target.Target) error
	WatchTargets(ctx context.Context) (<-chan TargetEvent, error)
	AcquireSessionLease(ctx context.Context, clientID string, ttl time.Duration) (Lease, error)
	PublishTargetHealth(ctx context.Context, id target.TargetID, state target.TargetState) error
	WatchTargetHealth(ctx context.Context) (<-chan TargetEvent, error)
}

type NoopStore struct{}

func (s *NoopStore) RegisterTarget(ctx context.Context, t target.Target) error {
	return nil
}
func (s *NoopStore) WatchTargets(ctx context.Context) (<-chan TargetEvent, error) {
	return make(chan TargetEvent), nil
}
func (s *NoopStore) AcquireSessionLease(ctx context.Context, clientID string, ttl time.Duration) (Lease, error) {
	return &noopLease{}, nil
}
func (s *NoopStore) PublishTargetHealth(ctx context.Context, id target.TargetID, state target.TargetState) error {
	return nil
}
func (s *NoopStore) WatchTargetHealth(ctx context.Context) (<-chan TargetEvent, error) {
	return make(chan TargetEvent), nil
}

type noopLease struct{}

func (l *noopLease) Refresh(ctx context.Context) error { return nil }
func (l *noopLease) Release() error                    { return nil }
func (l *noopLease) Key() string                        { return "" }
