package kv

import (
	"sync/atomic"
	"time"
)

const logicalBits = 16
const logicalMask = (1 << logicalBits) - 1

type Timestamp uint64

func compose(wall int64, logical uint16) Timestamp {
	return Timestamp((uint64(wall) << logicalBits) | uint64(logical))
}

func (t Timestamp) WallMillis() int64 { return int64(uint64(t) >> logicalBits) }
func (t Timestamp) Logical() uint16   { return uint16(uint64(t) & logicalMask) }

type Clock struct {
	last      atomic.Uint64
	MaxOffset time.Duration
}

func NewClock(maxOffset time.Duration) *Clock {
	c := &Clock{MaxOffset: maxOffset}
	now := time.Now().UnixMilli()
	c.last.Store(uint64(compose(now, 0)))
	return c
}

func (c *Clock) Now() Timestamp {
	for {
		phys := time.Now().UnixMilli()
		prev := Timestamp(c.last.Load())
		wall, log := prev.WallMillis(), prev.Logical()

		var next Timestamp
		if phys > wall {
			next = compose(phys, 0)
		} else {
			next = compose(wall, log+1)
		}
		if c.last.CompareAndSwap(uint64(prev), uint64(next)) {
			return next
		}
	}
}

func (c *Clock) Update(remote Timestamp) Timestamp {
	for {
		phys := time.Now().UnixMilli()
		prev := Timestamp(c.last.Load())
		pw, pl := prev.WallMillis(), prev.Logical()
		rw, rl := remote.WallMillis(), remote.Logical()

		w := max3(phys, pw, rw)
		var l uint16
		if w == pw && w == rw {
			l = max(pl, rl) + 1
		} else if w == pw {
			l = pl + 1
		} else if w == rw {
			l = rl + 1
		}
		next := compose(w, l)
		if c.last.CompareAndSwap(uint64(prev), uint64(next)) {
			return next
		}
	}
}

func max(a, b uint16) uint16 {
	if a > b {
		return a
	}
	return b
}
func max3(a, b, c int64) int64 {
	if a < b {
		a = b
	}
	if a < c {
		a = c
	}
	return a
}

func (c *Clock) CommitWait(commitTs Timestamp) {
	eps := 2 * c.MaxOffset
	target := time.UnixMilli(commitTs.WallMillis()).Add(eps)
	for time.Now().Before(target) {
		time.Sleep(time.Millisecond)
	}
}
