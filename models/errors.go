package models

import (
	"errors"
)

var (
	// ErrNotFound is returned when an aggregate does not exist.
	ErrNotFound = errors.New("asynx: aggregate not found")
	// ErrValidation is returned when a command fails validation.
	ErrValidation = errors.New("asynx: validation failed")
	// ErrPipelineFailed is returned when the command pipeline fails.
	ErrPipelineFailed = errors.New("asynx: pipeline failed")
	// ErrQueueFull is returned when a queue has no room for another request.
	ErrQueueFull = errors.New("asynx: queue full")
	// ErrShuttingDown is returned when work arrives during shutdown.
	ErrShuttingDown = errors.New("asynx: shutting down")
	// ErrAlreadyShuttingDown is returned when shutdown is requested twice.
	ErrAlreadyShuttingDown = errors.New("asynx: already shutting down")
	// ErrContextCancelled is returned when the context ends before the work completes.
	ErrContextCancelled = errors.New("asynx: context cancelled")
	// ErrBusClosed is returned when the bus is used after Close.
	ErrBusClosed = errors.New("asynx: bus closed")
	// ErrNilHandler is returned when a nil handler is supplied.
	ErrNilHandler = errors.New("asynx: handler is nil")
	// ErrEmptyPattern is returned when a subscription pattern is empty.
	ErrEmptyPattern = errors.New("asynx: pattern is empty")
	// ErrMissingEventStore is returned when no event store is configured.
	ErrMissingEventStore = errors.New("asynx: event store is required")
	// ErrMissingSnapshotStore is returned when no snapshot store is configured.
	ErrMissingSnapshotStore = errors.New("asynx: snapshot store is required")
	// ErrForgetFailed is returned when forgetting an aggregate fails.
	ErrForgetFailed = errors.New("asynx: forget failed")
	// ErrDispatcherClosed is returned when the dispatcher is used after Close.
	ErrDispatcherClosed = errors.New("asynx: dispatcher closed")
	// ErrDuplicateReceipt is returned when a receipt for the command already exists.
	ErrDuplicateReceipt = errors.New("asynx: duplicate command receipt")
	// ErrSyncProjection is returned when a synchronous projection fails.
	ErrSyncProjection = errors.New("asynx: sync projection failed")
	// ErrHandlerPanic is returned when a durable handler panics.
	ErrHandlerPanic = errors.New("asynx: durable handler panicked")
	// ErrHandlerTimeout is returned when a durable handler exceeds its timeout.
	ErrHandlerTimeout = errors.New("asynx: durable handler timed out")
	// ErrFallbackUnsupported is returned when WithFallback is used on a durable subscriber.
	ErrFallbackUnsupported = errors.New("asynx: durable subscribers do not support WithFallback")
	// ErrDeliveryParked is returned when the aggregate delivery is parked.
	ErrDeliveryParked = errors.New("asynx: durable delivery parked")
	// ErrForgetInProgress is returned when the aggregate is being forgotten.
	ErrForgetInProgress = errors.New("asynx: aggregate is being forgotten")
	// ErrSendWaitFromHandler is returned when a handler SendWaits to its own aggregate.
	ErrSendWaitFromHandler = errors.New("asynx: a handler cannot SendWait to the aggregate it is handling")
	// ErrForgetFromHandler is returned when a handler forgets its own aggregate.
	ErrForgetFromHandler = errors.New("asynx: a handler cannot forget the aggregate it is handling")
)
