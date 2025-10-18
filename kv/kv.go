package kv

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Version struct {
	TS    Timestamp
	Value string
}

type Entry struct {
	Key   string
	Value string
}

type Store struct {
	mu          sync.Mutex
	log         []Entry
	commitIndex int
	state       map[string]string

	readCond  *sync.Cond
	appliedCh chan struct{}
	versions  map[string][]Version

	clock *Clock
}

func (s *Store) Begin() *Txn {
	s.mu.Lock()
	start := s.clock.Now()
	s.mu.Unlock()

	return &Txn{
		s:       s,
		startTS: start,
		writes:  make(map[string]string),
	}
}

func New() *Store {
	s := &Store{
		state:       make(map[string]string),
		commitIndex: 0,
		versions:    make(map[string][]Version),
		clock:       NewClock(100 * time.Millisecond),
	}
	s.readCond = sync.NewCond(&s.mu)
	s.appliedCh = make(chan struct{})
	return s
}

func (s *Store) signalAppliedLocked() {
	close(s.appliedCh)
	s.appliedCh = make(chan struct{})
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

	// append a commited version
	commitTS := s.clock.Now()
	s.versions[e.Key] = append([]Version{{TS: commitTS, Value: e.Value}}, s.versions[e.Key]...)

	s.commitIndex++
	s.signalAppliedLocked()
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

	for s.commitIndex < target {
		ch := s.appliedCh
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
			// awoke because someone commited and channel was closed
		}
		s.mu.Lock()
	}
	s.mu.Unlock()
	return nil
}

func (s *Store) GetAt(key string, readTS Timestamp) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.versions[key] {
		if v.TS <= readTS {
			return v.Value, true
		}
	}
	return "", false
}

func (s *Store) GetLinearizable(ctx context.Context, key string) (string, bool, error) {
	fence := s.ReadFence()
	if err := s.WaitAppliedAtLeast(ctx, fence); err != nil {
		return "", false, err
	}
	v, ok := s.Get(key)
	return v, ok, nil
}
