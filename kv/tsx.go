package kv

import (
	"errors"
)

type Txn struct {
	s       *Store
	startTS Timestamp
	writes  map[string]string
}

func (tx *Txn) Put(key, val string) {
	tx.writes[key] = val
}

func (tx *Txn) Commit(ctx Context) error {
	tx.s.mu.Lock()
	defer tx.s.mu.Unlock()

	for k := range tx.writes {
		if vs := tx.s.versions[k]; len(vs) > 0 && vs[0].TS > tx.startTS {
			return errors.New("write-write conflict - key updated after txn start")
		}
	}

	commitTS := tx.s.clock.Now()

	for k, v := range tx.writes {
		tx.s.state[k] = v
		tx.s.versions[k] = append([]Version{{TS: commitTS, Value: v}}, tx.s.versions[k]...)
		tx.s.log = append(tx.s.log, Entry{Key: k, Value: v})
	}

	tx.s.commitIndex++
	tx.s.signalAppliedLocked()
	return nil
}
