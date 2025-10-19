package kv

import (
	"context"
	"testing"
	"time"
)

func TestCommitMatters(t *testing.T) {
	s := New()

	// 1) Put and read
	s.AppendPut("a", "1")
	if v, _ := s.Get("a"); v != "1" {
		t.Fatalf("after commit want 1, got %q", v)
	}

	// 2) Append pending, not committed yet -> state must still be old
	s.AppendPutPending("a", "2")
	if v, _ := s.Get("a"); v != "1" {
		t.Fatalf("before commit want 1, got %q", v)
	}

	// 3) Commit next, state updates
	if err := s.CommitNext(); err != nil {
		t.Fatal(err)
	}
	if v, _ := s.Get("a"); v != "2" {
		t.Fatalf("after commit want 2, got %q", v)
	}
}

func TestReadBarrierBlocksUntilFresh(t *testing.T) {
	s := New()
	s.AppendPut("a", "1")

	targetFence := s.ReadFence() + 1

	readDone := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.WaitAppliedAtLeast(ctx, targetFence); err != nil {
			t.Errorf("barrier wait failed: %v", err)
			close(readDone)
			return
		}
		v, _ := s.Get("a")
		if v != "2" {
			t.Errorf("after barrier want 2, got %q", v)
		}
		close(readDone)
	}()

	time.Sleep(50 * time.Millisecond)

	s.AppendPutPending("a", "2")

	select {
	case <-readDone:
		t.Fatal("reader should still be blocked before commit")
	case <-time.After(50 * time.Millisecond):
	}

	if err := s.CommitNext(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-readDone:
		// good
	case <-time.After(500 * time.Millisecond):
		t.Fatal("reader did not unblock after commit")
	}
}

func TestLinearizableReadUsesBarrier(t *testing.T) {
	s := New()
	s.AppendPut("k", "v1")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	v, ok, err := s.GetLinearizable(ctx, "k")
	if err != nil || !ok || v != "v1" {
		t.Fatalf("linearizable read failed: v=%q ok=%v err=%v", v, ok, err)
	}
}

func TestStaleVsLinearizableRead(t *testing.T) {
	s := New()
	s.AppendPut("k", "v1")
	s.AppendPutPending("k", "v2")

	// Sanity: stale-ok read sees old value before commit.
	if v, _ := s.Get("k"); v != "v1" {
		t.Fatalf("stale read should see old value before commit, got %q", v)
	}

	// 1) Linearizable read started BEFORE commit must NOT see v2.
	{
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		v, ok, err := s.GetLinearizable(ctx, "k")
		if err != nil || !ok {
			t.Fatalf("linearizable read failed. ok=%v, err=%v", ok, err)
		}
		if v != "v1" {
			t.Fatalf("linearizable read should not see uncommitted write; got %q", v)
		}
	}

	// Commit v2.
	if err := s.CommitNext(); err != nil {
		t.Fatal(err)
	}

	// 2) Linearizable read started AFTER commit must see v2.
	{
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		v, ok, err := s.GetLinearizable(ctx, "k")
		if err != nil || !ok || v != "v2" {
			t.Fatalf("after commit, linearizable read should see v2; got %q (ok=%v, err=%v)", v, ok, err)
		}
	}
}

func TestMVCCSnapshotRead(t *testing.T) {
	s := New()

	// Commit v1
	s.AppendPut("a", "v1")        // committed
	s.AppendPutPending("a", "v2") // staged but NOT committed yet

	// Take a snapshot timestamp after v1 is committed (and before v2 is committed)
	tsAfterV1 := s.clock.Now() // ok here because tests are in package kv

	require := func(ts Timestamp, want string) {
		if got, ok := s.GetAt("a", ts); !ok || got != want {
			t.Fatalf("GetAt(ts=%d): want %q, got %q (ok=%v)", ts, want, got, ok)
		}
	}

	// Snapshot after v1 should see v1
	require(tsAfterV1, "v1")

	// Now commit v2
	if err := s.CommitNext(); err != nil {
		t.Fatal(err)
	}

	// A timestamp from "now" should see v2 (the latest)
	tsAfterV2 := s.clock.Now()
	require(tsAfterV1, "v1") // still v1 when reading at the old snapshot
	require(tsAfterV2, "v2") // latest snapshot sees v2
}

func TestTxnOCCConflict(t *testing.T) {
	s := New()
	s.AppendPut("a", "v1") // ts=1

	tx := s.Begin() // startTS=1 (because commitIndex=1 right now)
	tx.Put("a", "v2")

	// Another writer commits "a"=v3 first (ts=2)
	s.AppendPut("a", "v3")

	// Now our txn must fail: a newer version exists (ts=2 > startTS=1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("expected OCC conflict, got nil")
	}
}

func TestHLCMonotonic(t *testing.T) {
	c := NewClock(50 * time.Millisecond)
	t1 := c.Now()
	time.Sleep(5 * time.Millisecond)
	t2 := c.Now()
	if t2.WallMillis() < t1.WallMillis() {
		t.Fatalf("time went backwards: %v -> %v", t1, t2)
	}
}

func TestCommitWait(t *testing.T) {
	c := NewClock(20 * time.Millisecond)
	ts := c.Now()
	epsilon := 2 * c.MaxOffset

	// Precompute the target wall time we must not return before
	target := time.UnixMilli(ts.WallMillis()).Add(epsilon)

	c.CommitWait(ts)

	// Assert the actual postcondition, not a wall-clock duration from an arbitrary start
	if time.Now().Before(target) {
		t.Fatalf("CommitWait returned too early: now=%v target=%v", time.Now(), target)
	}
}
