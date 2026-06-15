package buffer

import "testing"

func TestSizeClass(t *testing.T) {
	tests := []struct {
		in   int
		want int
	}{
		{1, 0},
		{256, 0},
		{257, 1},
		{512, 1},
		{513, 2},
		{4096, 4},
		{65536, 8},
	}
	for _, tt := range tests {
		got := SizeClass(tt.in)
		if got != tt.want {
			t.Errorf("SizeClass(%d) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestClassCapacity(t *testing.T) {
	tests := []struct {
		class int
		want  int
	}{
		{0, 256},
		{1, 512},
		{2, 1024},
		{8, 65536},
	}
	for _, tt := range tests {
		got := ClassCapacity(tt.class)
		if got != tt.want {
			t.Errorf("ClassCapacity(%d) = %d, want %d", tt.class, got, tt.want)
		}
	}
}

func TestPoolAcquireRelease(t *testing.T) {
	p := NewPool()
	buf := p.Acquire(128)
	if len(buf) != 128 {
		t.Errorf("len = %d, want 128", len(buf))
	}
	if cap(buf) != 256 {
		t.Errorf("cap = %d, want 256", cap(buf))
	}
	buf[0] = 42
	p.Release(buf)
}

func TestPoolHitRatio(t *testing.T) {
	p := NewPool()
	if ratio := p.HitRatio(); ratio != 1.0 {
		t.Errorf("empty pool ratio = %f, want 1.0", ratio)
	}
	p.Acquire(512)
	if ratio := p.HitRatio(); ratio != 0.0 {
		t.Errorf("first acquire ratio = %f, want 0.0", ratio)
	}
	buf := p.Acquire(512)
	p.Release(buf)
	p.Acquire(512)
	if ratio := p.HitRatio(); ratio <= 0.0 || ratio > 1.0 {
		t.Errorf("after warmup ratio = %f, want > 0", ratio)
	}
}
