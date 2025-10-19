package kv

import (
	"sync"
)

// in mem network where leader calls followers via fnc pointers
type Net struct {
	mu         sync.Mutex
	followers  []func(ctx Context, index int) error // to store rpc
	heartbeats []func(ctx Context) error            // heartbeat rpc
}

func NewNet() *Net { return &Net{} }

func (n *Net) AddFollower(store func(ctx Context, index int) error,
	heartbeat func(ctx Context) error) {
	n.mu.Lock()
	n.followers = append(n.followers, store)
	n.heartbeats = append(n.heartbeats, heartbeat)
	n.mu.Unlock()
}

func (n *Net) QuorumStore(ctx Context, index int) (acks int) {
	n.mu.Lock()
	fns := append([]func(Context, int) error{}, n.followers...)
	n.mu.Unlock()

	need := (len(fns) + 1) / 2
	acks = 1 // leader
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, fn := range fns {
		wg.Add(1)
		go func(call func(Context, int) error) {
			defer wg.Done()
			_ = call(ctx, index) // ignore error
			mu.Lock()
			acks++
			mu.Unlock()
		}(fn)
	}
	wg.Wait()
	if acks < need+1 { // leader + (need followers)
		return acks
	}
	return acks
}

func (n *Net) QuorumHeartbeat(ctx Context) bool {
	n.mu.Lock()
	fns := append([]func(Context) error{}, n.heartbeats...)
	n.mu.Unlock()

	need := (len(fns) + 1) / 2
	acks := 1 // leader
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, fn := range fns {
		wg.Add(1)
		go func(call func(Context) error) {
			defer wg.Done()
			_ = call(ctx)
			mu.Lock()
			acks++
			mu.Unlock()
		}(fn)
	}
	wg.Wait()
	return acks >= need+1
}
