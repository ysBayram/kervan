package netpoll

import (
	"testing"
)

func TestFDRegistryRegisterLookup(t *testing.T) {
	r := NewFDRegistry(1024)
	r.Register(42, "test-data")
	val := r.Lookup(42)
	if val == nil {
		t.Fatal("Lookup returned nil")
	}
	s, ok := val.(string)
	if !ok || s != "test-data" {
		t.Fatalf("got %v, want %q", val, "test-data")
	}
}

func TestFDRegistryUnregister(t *testing.T) {
	r := NewFDRegistry(1024)
	r.Register(42, "test-data")
	r.Unregister(42)
	if val := r.Lookup(42); val != nil {
		t.Fatal("expected nil after unregister")
	}
}

func TestFDRegistryOutOfBounds(t *testing.T) {
	r := NewFDRegistry(1024)
	if val := r.Lookup(99999); val != nil {
		t.Fatal("expected nil for out-of-bounds fd")
	}
	r.Register(99999, "data")
	if val := r.Lookup(99999); val != nil {
		t.Fatal("expected nil for out-of-bounds fd after register")
	}
}
