package queue

import (
	"testing"
)

func TestRoute_SameAggregate_AlwaysSameShard(
	t *testing.T,
) {
	r := mustRouter(
		t,
		8,
	)

	shard1 := r.Route("agg1")
	shard2 := r.Route("agg1")
	shard3 := r.Route("agg1")
	if shard1 != shard2 ||
		shard2 != shard3 {
		t.Errorf(
			"same aggregate routed to different shards: %d, %d, %d",
			shard1,
			shard2,
			shard3,
		)
	}
}

func TestRoute_ValidRange(
	t *testing.T,
) {
	r := mustRouter(
		t,
		8,
	)

	for i := 0; i < 100; i++ {
		checkInRange(
			t,
			r,
			"agg"+string(rune(i)),
		)
	}
}

func checkInRange(
	t *testing.T,
	r *Router,
	id string,
) {
	t.Helper()
	shard := r.Route(id)
	if shard < 0 ||
		shard >= 8 {
		t.Errorf(
			"invalid shard %d for aggregate %s",
			shard,
			id,
		)
	}
}

func TestRoute_SingleShard_AlwaysZero(
	t *testing.T,
) {
	r := mustRouter(
		t,
		1,
	)

	for i := 0; i < 50; i++ {
		checkZero(
			t,
			r,
			"agg"+string(rune(i)),
		)
	}
}

func checkZero(
	t *testing.T,
	r *Router,
	id string,
) {
	t.Helper()
	shard := r.Route(id)
	if shard != 0 {
		t.Errorf(
			"single shard router should always return 0, got %d",
			shard,
		)
	}
}

func TestNew_ErrorsOnZeroShards(
	t *testing.T,
) {
	if _, err := New(0); err == nil {
		t.Errorf("expected an error on numShards=0")
	}
}

func mustRouter(
	t *testing.T,
	numShards int,
) *Router {
	t.Helper()
	r, err := New(numShards)
	if err != nil {
		t.Fatalf(
			"New(%d): %v",
			numShards,
			err,
		)
	}

	return r
}
