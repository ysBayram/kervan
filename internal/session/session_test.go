package session

import (
	"testing"
)

func TestCASStateValidTransition(t *testing.T) {
	s := &ClientSession{}
	if !s.CASState(StateCreated, StateActive) {
		t.Error("Created -> Active should succeed")
	}
	if s.GetState() != StateActive {
		t.Errorf("state = %d, want %d", s.GetState(), StateActive)
	}
}

func TestCASStateInvalidTransition(t *testing.T) {
	s := &ClientSession{}
	s.State.Store(uint32(StateActive))
	if s.CASState(StateCreated, StateFrozen) {
		t.Error("Active -> Frozen from wrong base state should fail")
	}
}

func TestCASStateFrozenDraining(t *testing.T) {
	s := &ClientSession{}
	s.State.Store(uint32(StateFrozen))
	if !s.CASState(StateFrozen, StateDraining) {
		t.Error("Frozen -> Draining should succeed")
	}
}

func TestGetStateDefault(t *testing.T) {
	s := &ClientSession{}
	if s.GetState() != StateCreated {
		t.Errorf("default state = %d, want %d", s.GetState(), StateCreated)
	}
}
