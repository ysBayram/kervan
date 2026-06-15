package buffer

import "github.com/ysBayram/kervan/pkg/buffer"

const InlineThreshold = 256

type RingEntry struct {
	inline   [InlineThreshold]byte
	extBuf   []byte
	length   uint32
	seqNum   uint64
	enqueued int64
}

func (e *RingEntry) Payload() []byte {
	if e.extBuf != nil {
		return e.extBuf[:e.length]
	}
	return e.inline[:e.length]
}

func (e *RingEntry) Reset(pool *buffer.Pool) {
	if e.extBuf != nil {
		pool.Release(e.extBuf)
		e.extBuf = nil
	}
	e.length = 0
}
