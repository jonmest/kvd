package kv

import (
	"errors"
)

type Leader struct {
	Store *Store
	Net   *Net
}

// append -> quorum replicate -> commit -> apply
func (l *Leader) ProposePut(ctx Context, key, value string) error {
	// 1 Append locally as "pending"
	index := l.Store.AppendPutPending(key, value)

	// 2 Ask followers to durably store this index (quorum)
	if l.Net != nil {
		l.Net.QuorumStore(ctx, index+1) // +1 because commitIndex counts entries 1..N
	}

	// 3 Commit/apply locally
	return l.Store.CommitNext()
}

// ReadFence (quorum-backed): heartbeat followers to prove leadership "now"
func (l *Leader) ReadFence(ctx Context) (int, error) {
	if l.Net != nil && !l.Net.QuorumHeartbeat(ctx) {
		return 0, errors.New("not leader / no quorum")
	}
	// “Fresh” fence is the current commitIndex after a quorum heartbeat.
	return l.Store.ReadFence(), nil
}
