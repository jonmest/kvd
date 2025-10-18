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
