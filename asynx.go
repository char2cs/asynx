package asynx

// Asynx is the event-sourced facade for the aggregate type T: the one value an
// application holds to send commands, read aggregates and observe events. It is
// what Builder.Build returns, from a Builder that New creates.
//
// An aggregate is changed only by sending it commands (see models.Command).
// Each accepted command is validated against the aggregate's current state,
// appended to the event store as an immutable event, and then delivered to the
// subscribers: live projections that run in this process, and durable
// subscribers that are guaranteed every committed event. State is read back by
// replaying the events, with snapshots as a cache.
//
// The interface is the union of CommandAPI, which changes state or controls the
// instance, and QueryAPI, which reads state and observes events. Code that only
// needs one half can depend on that half, which also makes it easy to fake in
// tests.
//
// An Asynx is safe for concurrent use by multiple goroutines. Commands for one
// aggregate run one at a time, in the order they win the aggregate's lock;
// commands for different aggregates run in parallel (see ShardingOpts). After
// Shutdown has completed the instance no longer accepts commands.
type Asynx[T any] interface {
	CommandAPI[T]
	QueryAPI[T]
}
