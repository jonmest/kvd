package kv

import (
	"sync"
)

// simulates durably stored log on a follow for an entry
// only need to record index we stored to be able to ack
type FollowerDisk struct {
	mu         sync.Mutex
	lastStored int //highest stored log index
}

func (d *FollowerDisk) Store(index int) {
	d.mu.Lock()
	if index > d.lastStored {
		d.lastStored = index
	}
	d.mu.Unlock()
}

func (d *FollowerDisk) LastStored() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastStored
}
