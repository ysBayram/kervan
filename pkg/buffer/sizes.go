package buffer

const (
	InlineThreshold = 256
	MaxFrameSize    = 65536
	NumSizeClasses  = 9
)

func SizeClass(n int) int {
	switch {
	case n <= 256:
		return 0
	case n <= 512:
		return 1
	case n <= 1024:
		return 2
	case n <= 2048:
		return 3
	case n <= 4096:
		return 4
	case n <= 8192:
		return 5
	case n <= 16384:
		return 6
	case n <= 32768:
		return 7
	default:
		return 8
	}
}

func ClassCapacity(class int) int {
	return 256 << class
}
