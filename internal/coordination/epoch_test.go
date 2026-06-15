package coordination

import "testing"

func TestEpochIncrement(t *testing.T) {
	ew := NewEpochWatcher(nil)
	if ew.Current() != 0 {
		t.Errorf("initial epoch = %d, want 0", ew.Current())
	}
	v := ew.Increment()
	if v != 1 {
		t.Errorf("first increment = %d, want 1", v)
	}
	if ew.Current() != 1 {
		t.Errorf("after increment = %d, want 1", ew.Current())
	}
}

func TestEpochMonotonic(t *testing.T) {
	ew := NewEpochWatcher(nil)
	var prev uint64
	for i := 0; i < 100; i++ {
		v := ew.Increment()
		if v <= prev {
			t.Errorf("epoch not monotonic: %d <= %d", v, prev)
		}
		prev = v
	}
}
