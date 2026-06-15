package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ysBayram/kervan/internal/coordination"
	"github.com/ysBayram/kervan/internal/target"
)

type TargetRecord struct {
	ID        string `json:"id"`
	Addr      string `json:"addr"`
	Route     string `json:"route"`
	State     uint32 `json:"state"`
	NodeID    string `json:"node_id"`
	UpdatedAt int64  `json:"updated_at"`
}

type LeaseRecord struct {
	NodeID    string `json:"node_id"`
	Epoch     uint64 `json:"epoch"`
	CreatedAt int64  `json:"created_at"`
}

type EtcdStore struct {
	// endpoints  []string (stub)
	// client     *clientv3.Client (stub)
	nodeID string
}

func NewEtcdStore(endpoints []string, nodeID string) *EtcdStore {
	return &EtcdStore{nodeID: nodeID}
}

func (s *EtcdStore) RegisterTarget(ctx context.Context, t target.Target) error {
	rec := TargetRecord{
		ID:        string(t.ID),
		Addr:      t.Addr,
		Route:     t.Route,
		State:     uint32(t.GetState()),
		NodeID:    s.nodeID,
		UpdatedAt: time.Now().Unix(),
	}
	_, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal target record: %w", err)
	}
	return nil
}

func (s *EtcdStore) WatchTargets(ctx context.Context) (<-chan coordination.TargetEvent, error) {
	return make(chan coordination.TargetEvent), nil
}

func (s *EtcdStore) AcquireSessionLease(ctx context.Context, clientID string, ttl time.Duration) (coordination.Lease, error) {
	return &etcdLease{}, nil
}

func (s *EtcdStore) PublishTargetHealth(ctx context.Context, id target.TargetID, state target.TargetState) error {
	return nil
}

func (s *EtcdStore) WatchTargetHealth(ctx context.Context) (<-chan coordination.TargetEvent, error) {
	return make(chan coordination.TargetEvent), nil
}

type etcdLease struct{}

func (l *etcdLease) Refresh(ctx context.Context) error { return nil }
func (l *etcdLease) Release() error                    { return nil }
func (l *etcdLease) Key() string                        { return "" }
