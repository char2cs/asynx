package asynx

import (
	"errors"
	"time"
)

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

// Command defines the contract for aggregate mutations.
// Implementations must be pure — no IO, side effects, or randomness.
type Command[T any] interface {
	AggregateID() string
	EventName() string
	ShouldSnapshot() bool

	// Validate receives nil current when the aggregate has never existed.
	Validate(
		current *T,
	) error

	// EmitEvent receives nil current when the aggregate has never existed.
	EmitEvent(
		current *T,
	) T
}

var (
	ErrNotFound            = errors.New("asynx: aggregate not found")
	ErrValidation          = errors.New("asynx: validation failed")
	ErrPipelineFailed      = errors.New("asynx: pipeline failed")
	ErrQueueFull           = errors.New("asynx: queue full")
	ErrShuttingDown        = errors.New("asynx: shutting down")
	ErrAlreadyShuttingDown = errors.New("asynx: already shutting down")
	ErrContextCancelled    = errors.New("asynx: context cancelled")
	ErrBusClosed           = errors.New("asynx: bus closed")
	ErrNilHandler          = errors.New("asynx: handler is nil")
	ErrEmptyPattern        = errors.New("asynx: pattern is empty")
	ErrMissingEventStore   = errors.New("asynx: event store is required")
)
