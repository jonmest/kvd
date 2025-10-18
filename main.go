package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jonmest/kvd/kv"
)

func main() {
	s := kv.New()
	s.AppendPut("user:42", "alice")
	v, _ := s.Get("user:42")
	fmt.Println("after PUT, Get:", v) // alice

	// Simulate in-flight write
	s.AppendPutPending("user:42", "bob")
	v, _ = s.Get("user:42")
	fmt.Println("pending write, Get still:", v) // alice

	// Show linearizable shape (currently same fence; will be Raft later)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	v, ok, err := s.GetLinearizable(ctx, "user:42")
	fmt.Println("linearizable read:", v, ok, err)

	// Commit; state updates
	_ = s.CommitNext()
	v, _ = s.Get("user:42")
	fmt.Println("after commit, Get:", v) // bob
}
