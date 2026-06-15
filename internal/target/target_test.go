package target

import "testing"

func TestTargetStateMachine(t *testing.T) {
	tgt := &Target{ID: "test-1"}

	if s := tgt.GetState(); s != TargetUnknown {
		t.Errorf("initial state = %d, want %d", s, TargetUnknown)
	}

	if !tgt.CASState(TargetUnknown, TargetHealthy) {
		t.Error("Unknown -> Healthy should succeed")
	}
	if s := tgt.GetState(); s != TargetHealthy {
		t.Errorf("state = %d, want %d", s, TargetHealthy)
	}

	if !tgt.CASState(TargetHealthy, TargetUnhealthy) {
		t.Error("Healthy -> Unhealthy should succeed")
	}

	if tgt.CASState(TargetHealthy, TargetUnhealthy) {
		t.Error("Unhealthy -> Unhealthy from wrong base should fail")
	}
}
