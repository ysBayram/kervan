package buffer

import (
	"sync"
	"sync/atomic"
)

type Pool struct {
	pools  [NumSizeClasses]sync.Pool
	hits   atomic.Uint64
	misses atomic.Uint64
}

func NewPool() *Pool {
	return &Pool{}
}

func (p *Pool) Acquire(size int) []byte {
	class := SizeClass(size)
	buf := p.pools[class].Get()
	if buf == nil {
		p.misses.Add(1)
		return make([]byte, ClassCapacity(class))[:size]
	}
	p.hits.Add(1)
	return buf.([]byte)[:size]
}

func (p *Pool) Release(buf []byte) {
	class := SizeClass(cap(buf))
	p.pools[class].Put(buf[:cap(buf)])
}

func (p *Pool) HitRatio() float64 {
	h := p.hits.Load()
	m := p.misses.Load()
	if h+m == 0 {
		return 1.0
	}
	return float64(h) / float64(h+m)
}
