package kv

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Entry struct {
	Key   string
	Value string
}

type Store struct {
	mu          sync.Mutex
	log         []Entry
	commitIndex int
	state       map[string]string

	readCond *sync.Cond
}

func New() *Store {
	s := &Store{
		state:       make(map[string]string),
		commitIndex: 0,
	}
	s.readCond = sync.NewCond(&s.mu)
	return s
}

func (s *Store) AppendPutPending(key, value string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, Entry{Key: key, Value: value})
	return len(s.log) - 1 // index of appended entry
}

func (s *Store) CommitNext() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.commitIndex >= len(s.log) {
		return errors.New("nothing to commit")
	}
	e := s.log[s.commitIndex]
	s.state[e.Key] = e.Value
	s.commitIndex++
	s.readCond.Broadcast()
	return nil
}

func (s *Store) AppendPut(key, value string) {
	_ = s.AppendPutPending(key, value)
	_ = s.CommitNext()
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.state[key]
	return v, ok
}

func (s *Store) ReadFence() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitIndex
}

func (s *Store) WaitAppliedAtLeast(ctx context.Context, target int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.commitIndex < target {
		if err := ctx.Err(); err != nil {
			return err
		}
		done := make(chan struct{})
		go func() {
			s.readCond.Wait()
			close(done)
		}()
		select {
		case <-done:
			// woke up by Broadcast or spurious wake; loop continues
		case <-ctx.Done():
			// Wake the waiter goroutine by broadcasting and return.
			s.readCond.Broadcast()
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
			// Periodic wake to re-check ctx (defensive).
		}
	}
	return nil
}

func (s *Store) GetLinearizable(ctx context.Context, key string) (string, bool, error) {
	fence := s.ReadFence()
	if err := s.WaitAppliedAtLeast(ctx, fence); err != nil {
		return "", false, err
	}
	v, ok := s.Get(key)
	return v, ok, nil
}
