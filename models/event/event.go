package event

import (
	"time"
)

// Event describes a change to an aggregate at one version. It is what
// subscribers receive and what Send returns once the change is durably
// written.
//
// ID is unique per event. AggregateID names the aggregate and Version is the
// event's position in that aggregate's stream, starting at 1 and rising by one
// per event, so (AggregateID, Version) identifies an event and is what a
// durable handler dedupes on. EventName is what subscribers are matched
// against. SchemaVersion is the version of the aggregate's schema the event
// was written under, which upcasters migrate from. OccurredAt is the time the
// event was committed.
//
// Aggregate is the aggregate's state after the event and PreviousAggregate its
// state before it; for the first event of an aggregate PreviousAggregate is
// the zero value of T.
type Event[T any] struct {
	ID                string
	AggregateID       string
	EventName         string
	Version           int64
	SchemaVersion     int
	OccurredAt        time.Time
	Aggregate         T
	PreviousAggregate T
}
