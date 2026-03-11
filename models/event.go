package models

import "time"

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
