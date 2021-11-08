package atomic

import "sync/atomic"

type AtomicCounter struct {
	v uint64
}

func NewCounter(initial uint64) *AtomicCounter {
	return &AtomicCounter{v: initial}
}

func (c *AtomicCounter) Incr() uint64 {
	return atomic.AddUint64(&c.v, 1)
}

func (c *AtomicCounter) Decr() uint64 {
	return atomic.AddUint64(&c.v, ^uint64(0))
}

func (c *AtomicCounter) Add(v uint64) uint64 {
	return atomic.AddUint64(&c.v, v)
}

func (c *AtomicCounter) Get() uint64 {
	return atomic.LoadUint64(&c.v)
}

func (c *AtomicCounter) CompareAndSwap(o, n uint64) bool {
	return atomic.CompareAndSwapUint64(&c.v, o, n)
}