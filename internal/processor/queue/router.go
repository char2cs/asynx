// Package queue implements consistent shard routing for Asynx commands.
//
// Router uses FNV-1a hash to deterministically route aggregates to shards.
// The same aggregateID always hashes to the same shard, ensuring commands
// for the same aggregate are processed serially by that shard's dispatcher.
//   - Hash function — FNV-1a 64-bit; portable and fast
//   - Modulo mapping — hash % numShards yields shard index [0, numShards)
//
// Router is stateless; Create/Read/Update/Delete operations never collide as
// long as numShards remains constant. Resharding is out of scope.
package queue

import "hash/fnv"

type Router struct {
	numShards int
}

func New(numShards int) *Router {
	if numShards == 0 {
		panic("numShards must be > 0")
	}
	return &Router{
		numShards: numShards,
	}
}

func (r *Router) Route(aggregateID string) int {
	h := fnv.New64a()
	h.Write(
		[]byte(aggregateID),
	)
	return int(h.Sum64() % uint64(r.numShards))
}
