package models

import "time"

// Event describes a change to an aggregate at one version.
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
