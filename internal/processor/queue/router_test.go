package queue

import (
	"testing"
)

func TestRoute_SameAggregate_AlwaysSameShard(t *testing.T) {
	r := New(8)

	shard1 := r.Route("agg1")
	shard2 := r.Route("agg1")
	shard3 := r.Route("agg1")

	if shard1 != shard2 || shard2 != shard3 {
		t.Errorf("same aggregate routed to different shards: %d, %d, %d", shard1, shard2, shard3)
	}
}

func TestRoute_ValidRange(t *testing.T) {
	r := New(8)

	for i := 0; i < 100; i++ {
		id := "agg" + string(rune(i))
		shard := r.Route(id)
		if shard < 0 || shard >= 8 {
			t.Errorf("invalid shard %d for aggregate %s", shard, id)
		}
	}
}

func TestRoute_SingleShard_AlwaysZero(t *testing.T) {
	r := New(1)

	for i := 0; i < 50; i++ {
		id := "agg" + string(rune(i))
		shard := r.Route(id)
		if shard != 0 {
			t.Errorf("single shard router should always return 0, got %d", shard)
		}
	}
}

func TestNew_PanicsOnZeroShards(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic on numShards=0")
		}
	}()

	New(0)
}
