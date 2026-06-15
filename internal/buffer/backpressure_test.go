package buffer

import "testing"

func TestParsePolicy(t *testing.T) {
	tests := []struct {
		s    string
		want BackpressurePolicy
		err  bool
	}{
		{"drop_oldest", DropOldest, false},
		{"reject", Reject, false},
		{"block", Block, false},
		{"invalid", DropOldest, true},
	}
	for _, tt := range tests {
		got, err := ParsePolicy(tt.s)
		if tt.err {
			if err == nil {
				t.Errorf("ParsePolicy(%q): expected error", tt.s)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParsePolicy(%q): %v", tt.s, err)
		}
		if got != tt.want {
			t.Errorf("ParsePolicy(%q) = %d, want %d", tt.s, got, tt.want)
		}
	}
}
