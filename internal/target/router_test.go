package target

import "testing"

func TestRouterConsistentHash(t *testing.T) {
	r := NewRouter()
	targets := []TargetID{"a", "b", "c"}
	r.UpdateRoute("default", targets)

	id1, _ := r.Select("default", "client-1")
	id2, _ := r.Select("default", "client-1")
	if id1 != id2 {
		t.Errorf("same client mapped to different targets: %s vs %s", id1, id2)
	}
}

func TestRouterSkipsUnhealthy(t *testing.T) {
	r := NewRouter()
	targets := []TargetID{"a", "b", "c"}
	r.UpdateRoute("default", targets)

	isHealthy := func(id TargetID) bool {
		return id != "a"
	}

	id, err := r.SelectHealthy("default", "test", isHealthy)
	if err != nil {
		t.Fatalf("SelectHealthy: %v", err)
	}
	if id == "a" {
		t.Error("selected unhealthy target 'a'")
	}
}

func TestRouterNoTargets(t *testing.T) {
	r := NewRouter()
	if _, err := r.Select("default", "test"); err != ErrNoHealthyTargets {
		t.Errorf("expected ErrNoHealthyTargets, got %v", err)
	}
}
