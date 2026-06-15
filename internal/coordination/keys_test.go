package coordination

import "testing"

func TestKeyFormatting(t *testing.T) {
	if KeyPrefix != "kervan/" {
		t.Errorf("KeyPrefix = %q, want %q", KeyPrefix, "kervan/")
	}
	if KeyTarget != "kervan/targets/" {
		t.Errorf("KeyTarget = %q, want %q", KeyTarget, "kervan/targets/")
	}
	if KeyLease != "kervan/leases/" {
		t.Errorf("KeyLease = %q, want %q", KeyLease, "kervan/leases/")
	}
}

func TestKeyPaths(t *testing.T) {
	if KeyTarget+"my-target" != "kervan/targets/my-target" {
		t.Errorf("unexpected target key path")
	}
	if KeyLease+"client-1" != "kervan/leases/client-1" {
		t.Errorf("unexpected lease key path")
	}
}
