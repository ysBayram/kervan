package session

import (
	"sync"
	"testing"

	"github.com/ysBayram/kervan/internal/config"
)

func newTestManager() *SessionManager {
	return NewManager(config.SessionConfig{ShardCount: 256})
}

func TestInsertAndGet(t *testing.T) {
	sm := newTestManager()
	sess := &ClientSession{ClientID: "test-client"}
	if err := sm.Insert(sess); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	got, ok := sm.Get("test-client")
	if !ok {
		t.Fatal("Get returned not found")
	}
	if got.ClientID != "test-client" {
		t.Errorf("ClientID = %q, want %q", got.ClientID, "test-client")
	}
	if sm.ActiveCount() != 1 {
		t.Errorf("ActiveCount = %d, want 1", sm.ActiveCount())
	}
}

func TestInsertDuplicateRejected(t *testing.T) {
	sm := newTestManager()
	sess1 := &ClientSession{ClientID: "dup"}
	sess2 := &ClientSession{ClientID: "dup"}
	if err := sm.Insert(sess1); err != nil {
		t.Fatal(err)
	}
	if err := sm.Insert(sess2); err != ErrDuplicateClientID {
		t.Errorf("expected ErrDuplicateClientID, got %v", err)
	}
}

func TestRemove(t *testing.T) {
	sm := newTestManager()
	sess := &ClientSession{ClientID: "remove-me"}
	sm.Insert(sess)
	removed, ok := sm.Remove("remove-me")
	if !ok {
		t.Fatal("Remove returned not found")
	}
	if removed.ClientID != "remove-me" {
		t.Errorf("ClientID = %q", removed.ClientID)
	}
	if sm.ActiveCount() != 0 {
		t.Errorf("ActiveCount = %d, want 0", sm.ActiveCount())
	}
	if _, ok := sm.Get("remove-me"); ok {
		t.Error("session still exists after remove")
	}
}

func TestConcurrentGetInsert(t *testing.T) {
	sm := newTestManager()
	var wg sync.WaitGroup
	n := 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cid := string(rune(id))
			sess := &ClientSession{ClientID: cid}
			_ = sm.Insert(sess)
			sm.Get(cid)
		}(i)
	}
	wg.Wait()
	if sm.ActiveCount() == 0 {
		t.Error("expected active sessions")
	}
}
