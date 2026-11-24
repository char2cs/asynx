package queue

import (
	"errors"
	"hash/fnv"
)

var errNoShards = errors.New("numShards must be > 0")

// Router maps an aggregate id to a shard index with a stable hash, so all
// commands of one aggregate reach the same shard and are serialised there,
// while different aggregates spread across shards. It is immutable and safe for
// concurrent use. Build it with New.
type Router struct {
	numShards int
}

// New builds a Router over numShards shards. It fails when numShards is not
// positive, since there would be nowhere to route to.
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

// Route returns the index, in [0, numShards), of the shard that owns
// aggregateID. The same id always yields the same index for a given Router.
func (r *Router) Route(
	aggregateID string,
) int {
	h := fnv.New64a()
	h.Write(
		[]byte(aggregateID),
	)
	return int(h.Sum64() % uint64(r.numShards))
}
