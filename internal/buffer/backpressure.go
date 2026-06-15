package buffer

import (
	"errors"
	"fmt"
	"strings"
)

type BackpressurePolicy int

const (
	DropOldest BackpressurePolicy = iota
	Reject
	Block
)

var (
	ErrBufferFull   = errors.New("buffer full")
	ErrFrameTooLarge = errors.New("frame exceeds max size")
	ErrWouldBlock   = errors.New("operation would block")
)

func ParsePolicy(s string) (BackpressurePolicy, error) {
	switch strings.ToLower(s) {
	case "drop_oldest":
		return DropOldest, nil
	case "reject":
		return Reject, nil
	case "block":
		return Block, nil
	default:
		return DropOldest, fmt.Errorf("unknown backpressure policy: %s", s)
	}
}
