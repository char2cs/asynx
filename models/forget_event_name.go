package models

// ForgetEventName is the EventName of the tombstone Forget writes. Every
// durable subscriber receives it, whatever its pattern, so it can drop
// per-aggregate state such as an (AggregateID, Version) dedupe table: a
// re-created aggregate restarts at version 1, and without that a handler would
// mistake its first events for duplicates. Handlers of other kinds can match
// it by name like any event.
const ForgetEventName = "asynx.aggregate.forget"
