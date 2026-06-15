package redis

import (
	"context"
	"time"

	"github.com/ysBayram/kervan/internal/coordination"
	"github.com/ysBayram/kervan/internal/target"
)

type RedisStore struct {
	// client *redis.ClusterClient (stub)
}

func NewRedisStore(addrs []string) *RedisStore {
	return &RedisStore{}
}

func (s *RedisStore) RegisterTarget(ctx context.Context, t target.Target) error {
	return nil
}

func (s *RedisStore) WatchTargets(ctx context.Context) (<-chan coordination.TargetEvent, error) {
	return make(chan coordination.TargetEvent), nil
}

func (s *RedisStore) AcquireSessionLease(ctx context.Context, clientID string, ttl time.Duration) (coordination.Lease, error) {
	return &redisLease{}, nil
}

func (s *RedisStore) PublishTargetHealth(ctx context.Context, id target.TargetID, state target.TargetState) error {
	return nil
}

func (s *RedisStore) WatchTargetHealth(ctx context.Context) (<-chan coordination.TargetEvent, error) {
	return make(chan coordination.TargetEvent), nil
}

type redisLease struct{}

func (l *redisLease) Refresh(ctx context.Context) error { return nil }
func (l *redisLease) Release() error                    { return nil }
func (l *redisLease) Key() string                        { return "" }
