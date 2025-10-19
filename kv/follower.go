package kv

type Follower struct {
	Disk *FollowerDisk
}

func (f *Follower) StoreRPC(ctx Context, index int) error {
	f.Disk.Store(index)
	return nil
}
func (f *Follower) HeartbeatRPC(ctx Context) error {
	// Could check liveness/state; here we just ack.
	return nil
}
