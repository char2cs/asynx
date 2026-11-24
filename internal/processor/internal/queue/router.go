package queue

import (
	"errors"
	"hash/fnv"
)

var errNoShards = errors.New("numShards must be > 0")

// Router maps an aggregate ID to a shard index.
type Router struct {
	numShards int
}

// New builds a Router over numShards shards and fails when there are none.
func New(
	numShards int,
) (*Router, error) {
	if numShards <= 0 {
		return nil,
			errNoShards
	}

	return &Router{
			numShards: numShards,
		},
		nil
}

// Route returns the shard index that owns aggregateID.
func (r *Router) Route(
	aggregateID string,
) int {
	h := fnv.New64a()
	h.Write(
		[]byte(aggregateID),
	)
	return int(h.Sum64() % uint64(r.numShards))
}
