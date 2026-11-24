package asynx

// ShardingOpts configures the processor's worker pool: how many shards the
// aggregates are spread over, how many commands may wait, and how many workers
// serve each shard. The zero value is valid and uses the defaults below.
//
// WorkersPerShard tunes throughput only: whatever its value, commands for one
// aggregate run strictly one at a time (a per-aggregate lock covers load,
// validate, commit and enqueue), so there is no lost-update or version-conflict
// window and handlers receive an aggregate's events in version order. Commands
// for different aggregates run in parallel up to Shards*WorkersPerShard: a
// command waiting for a busy aggregate does not hold a worker, so one slow
// aggregate occupies at most one of its shard's workers.
//
// Work inside a store transaction (Builder.WithSyncProjection) is not covered:
// a store that runs one transaction at a time serialises every command behind
// it. The lock is in-process: several processes sharing one store are protected
// from corruption by the store's version check, but not from conflicts.
type ShardingOpts struct {
	// Shards is the number of shards the aggregates are hashed over. It
	// defaults to 8 when left zero.
	Shards int

	// QueueDepth bounds the commands that may wait behind the one running for
	// an aggregate: further commands to it are refused with
	// models.ErrQueueFull, and other aggregates keep their workers. It is also
	// the size of the shard's intake buffer, which all of the shard's
	// aggregates share: while a flood fills it, a command to another aggregate
	// on the same shard can be refused with models.ErrQueueFull too, so size it
	// for the bursts you expect. It defaults to WorkersPerShard when left zero
	// and is never less than it. The queue is bounded: it is not unlimited
	// when zero.
	QueueDepth int

	// WorkersPerShard is how many workers drain each shard's queue. It
	// defaults to 8 when left zero.
	WorkersPerShard int
}
