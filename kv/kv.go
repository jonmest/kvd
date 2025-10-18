package kv

import (
	"context"
	"errors"
	"sync"
)

type Timestamp = int

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
}

type Txn struct {
	s       *Store
	startTS Timestamp
	writes  map[string]string
}

func (s *Store) Begin() *Txn {
	s.mu.Lock()
	start := s.commitIndex
	s.mu.Unlock()

	return &Txn{
		s:       s,
		startTS: start,
		writes:  make(map[string]string),
	}
}

func (tx *Txn) Put(key, val string) {
	tx.writes[key] = val
}

func (tx *Txn) Commit(ctx context.Context) error {
	tx.s.mu.Lock()
	defer tx.s.mu.Unlock()

	for k := range tx.writes {
		vs := tx.s.versions[k]
		if len(vs) > 0 && vs[0].TS > tx.startTS {
			return errors.New("write-write conflict - key updated after txn start")
		}
	}

	commitTS := tx.s.commitIndex + 1
	for k, v := range tx.writes {
		tx.s.state[k] = v
		tx.s.versions[k] = append([]Version{{TS: commitTS, Value: v}}, tx.s.versions[k]...)
		tx.s.log = append(tx.s.log, Entry{Key: k, Value: v})
	}

	tx.s.commitIndex++
	tx.s.signalAppliedLocked()
	return nil
}

func New() *Store {
	s := &Store{
		state:       make(map[string]string),
		commitIndex: 0,
		versions:    make(map[string][]Version),
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
	commitTS := s.commitIndex + 1
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
