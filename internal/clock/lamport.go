package clock

import "sync/atomic"

type LamportClock struct {
	counter atomic.Int64
}

func NewLamportClock() *LamportClock {
	return &LamportClock{}
}

func (c *LamportClock) Increment() int64 {
	return c.counter.Add(1)
}

func (c *LamportClock) Update(incoming int64) {
	for {
		current := c.counter.Load()
		if current >= incoming {
			break
		}
		if c.counter.CompareAndSwap(current, incoming) {
			break
		}
	}
}
