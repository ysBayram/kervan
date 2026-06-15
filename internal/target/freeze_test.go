package target

import (
	"testing"

	"github.com/ysBayram/kervan/internal/session"
)

func TestFreezeCAS(t *testing.T) {
	s := &session.ClientSession{}
	// Session starts in Created, set to Active
	s.State.Store(uint32(session.StateActive))

	fc := &FreezeController{}
	fc.freezeSession(s)

	if s.GetState() != session.StateFrozen {
		t.Errorf("state = %d, want %d", s.GetState(), session.StateFrozen)
	}
}

func TestFreezeOnlyActive(t *testing.T) {
	s := &session.ClientSession{}
	s.State.Store(uint32(session.StateFrozen))

	fc := &FreezeController{}
	fc.freezeSession(s)

	if s.GetState() != session.StateFrozen {
		t.Errorf("state should remain frozen, got %d", s.GetState())
	}
}
