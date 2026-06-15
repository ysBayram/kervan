package flush

import (
	"errors"
	"sync/atomic"

	"github.com/ysBayram/kervan/internal/buffer"
	"github.com/ysBayram/kervan/internal/session"
	"github.com/ysBayram/kervan/pkg/buffer"
)

var ErrShortWrite = errors.New("short write to upstream")

type Metrics struct {
	FlushTotal atomic.Uint64
}

type Scheduler struct {
	metrics *Metrics
	writer  UpstreamWriter
}

type UpstreamWriter interface {
	WriteFrame(sess *session.ClientSession, payload []byte) (int, error)
}

func NewScheduler(writer UpstreamWriter) *Scheduler {
	return &Scheduler{
		metrics: &Metrics{},
		writer:  writer,
	}
}

func (fs *Scheduler) Drain(sess *session.ClientSession, pool *buffer.Pool) error {
	for {
		entry, ok := sess.RingBuffer.Peek()
		if !ok {
			return nil
		}

		n, err := fs.writer.WriteFrame(sess, entry.Payload())
		if err != nil {
			return err
		}
		if n != len(entry.Payload()) {
			return ErrShortWrite
		}

		sess.RingBuffer.AdvanceHead()
		fs.metrics.FlushTotal.Add(1)
	}
}

func (fs *Scheduler) Metrics() *Metrics {
	return fs.metrics
}
