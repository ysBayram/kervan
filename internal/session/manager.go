package session

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/ysBayram/kervan/internal/config"
)

var ErrDuplicateClientID = errors.New("duplicate client ID")
var ErrSessionNotFound = errors.New("session not found")

type shard struct {
	mu       sync.RWMutex
	sessions map[string]*ClientSession
}

type SessionManager struct {
	shards    []*shard
	shardMask uint32
	active    atomic.Int64
}

func NewManager(cfg config.SessionConfig) *SessionManager {
	count := cfg.ShardCount
	if count == 0 {
		count = DefaultShardCount
	}
	if count&(count-1) != 0 {
		count = 256
	}
	shards := make([]*shard, count)
	for i := range shards {
		shards[i] = &shard{sessions: make(map[string]*ClientSession)}
	}
	return &SessionManager{
		shards:    shards,
		shardMask: count - 1,
	}
}

func (sm *SessionManager) shardFor(clientID string) *shard {
	return sm.shards[ShardIndex(clientID, sm.shardMask)]
}

func (sm *SessionManager) shardByIndex(idx uint8) *shard {
	return sm.shards[idx]
}

func (sm *SessionManager) Get(clientID string) (*ClientSession, bool) {
	s := sm.shardFor(clientID)
	s.mu.RLock()
	sess, ok := s.sessions[clientID]
	s.mu.RUnlock()
	return sess, ok
}

func (sm *SessionManager) Insert(sess *ClientSession) error {
	s := sm.shardFor(sess.ClientID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.sessions[sess.ClientID]; exists {
		return ErrDuplicateClientID
	}
	s.sessions[sess.ClientID] = sess
	sm.active.Add(1)
	return nil
}

func (sm *SessionManager) Remove(clientID string) (*ClientSession, bool) {
	s := sm.shardFor(clientID)
	s.mu.Lock()
	sess, ok := s.sessions[clientID]
	if ok {
		delete(s.sessions, clientID)
		sm.active.Add(-1)
	}
	s.mu.Unlock()
	return sess, ok
}

func (sm *SessionManager) ActiveCount() int64 {
	return sm.active.Load()
}

func (sm *SessionManager) RangeShard(idx uint8, fn func(*ClientSession) bool) {
	if int(idx) >= len(sm.shards) {
		return
	}
	s := sm.shards[idx]
	s.mu.RLock()
	for _, sess := range s.sessions {
		if !fn(sess) {
			break
		}
	}
	s.mu.RUnlock()
}

func (sm *SessionManager) RangeAll(fn func(*ClientSession) bool) {
	for _, s := range sm.shards {
		s.mu.RLock()
		for _, sess := range s.sessions {
			if !fn(sess) {
				s.mu.RUnlock()
				return
			}
		}
		s.mu.RUnlock()
	}
}

func (sm *SessionManager) CountByTarget(targetID string) int {
	var count int
	sm.RangeAll(func(sess *ClientSession) bool {
		if sess.TargetID == targetID {
			count++
		}
		return true
	})
	return count
}

func (sm *SessionManager) CountByState(state State) int64 {
	var count int64
	sm.RangeAll(func(sess *ClientSession) bool {
		if sess.GetState() == state {
			count++
		}
		return true
	})
	return count
}
