package session

import (
	"math"
	"testing"
)

func TestShardIndexDistribution(t *testing.T) {
	mask := uint32(255)
	counts := make([]int, 256)
	n := 100000
	for i := 0; i < n; i++ {
		id := "client-" + string(rune(i))
		idx := ShardIndex(id, mask)
		counts[idx]++
	}
	expected := n / 256
	tolerance := expected / 2
	for i, c := range counts {
		if math.Abs(float64(c-expected)) > float64(tolerance) {
			t.Errorf("shard %d: count = %d, expected ~%d (±%d)", i, c, expected, tolerance)
		}
	}
}

func TestShardIndexDeterministic(t *testing.T) {
	mask := uint32(255)
	a := ShardIndex("test-client", mask)
	b := ShardIndex("test-client", mask)
	if a != b {
		t.Errorf("not deterministic: %d vs %d", a, b)
	}
}
