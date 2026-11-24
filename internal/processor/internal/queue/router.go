package queue

import "hash/fnv"

type Router struct {
	numShards int
}

func New(
	numShards int,
) *Router {
	if numShards == 0 {
		panic("numShards must be > 0")
	}

	return &Router{
		numShards: numShards,
	}
}

func (r *Router) Route(
	aggregateID string,
) int {
	h := fnv.New64a()
	h.Write(
		[]byte(aggregateID),
	)
	return int(h.Sum64() % uint64(r.numShards))
}
