package kv

import (
	"context"
	"testing"
	"time"
)

func TestQuorumCommitAndLinearizableReadFence(t *testing.T) {
	// Leader with a store
	s := New()
	leader := &Leader{Store: s, Net: NewNet()}

	// Two followers
	f1 := &Follower{Disk: &FollowerDisk{}}
	f2 := &Follower{Disk: &FollowerDisk{}}

	// Register follower RPCs
	leader.Net.AddFollower(f1.StoreRPC, f1.HeartbeatRPC)
	leader.Net.AddFollower(f2.StoreRPC, f2.HeartbeatRPC)

	// 1) Propose a write
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := leader.ProposePut(ctx, "x", "1"); err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	// It should be visible via linearizable read (fence + barrier)
	fence, err := leader.ReadFence(ctx)
	if err != nil {
		t.Fatalf("read fence failed: %v", err)
	}
	if err := leader.Store.WaitAppliedAtLeast(ctx, fence); err != nil {
		t.Fatalf("barrier failed: %v", err)
	}
	if v, _ := leader.Store.Get("x"); v != "1" {
		t.Fatalf("want 1, got %q", v)
	}
}
