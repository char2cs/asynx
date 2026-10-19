package models

import (
	"errors"
	"fmt"
)

var (
	ErrNotFound             = errors.New("asynx: aggregate not found")
	ErrValidation           = errors.New("asynx: validation failed")
	ErrPipelineFailed       = errors.New("asynx: pipeline failed")
	ErrQueueFull            = errors.New("asynx: queue full")
	ErrShuttingDown         = errors.New("asynx: shutting down")
	ErrAlreadyShuttingDown  = errors.New("asynx: already shutting down")
	ErrContextCancelled     = errors.New("asynx: context cancelled")
	ErrBusClosed            = errors.New("asynx: bus closed")
	ErrNilHandler           = errors.New("asynx: handler is nil")
	ErrEmptyPattern         = errors.New("asynx: pattern is empty")
	ErrMissingEventStore    = errors.New("asynx: event store is required")
	ErrMissingSnapshotStore = errors.New("asynx: snapshot store is required")
	ErrForgetFailed         = errors.New("asynx: forget failed")
	ErrDispatcherClosed     = errors.New("asynx: dispatcher closed")
	ErrDuplicateReceipt     = errors.New("asynx: duplicate command receipt")
	ErrSyncProjection       = errors.New("asynx: sync projection failed")
	ErrHandlerPanic         = errors.New("asynx: durable handler panicked")
	ErrHandlerTimeout       = errors.New("asynx: durable handler timed out")
	ErrFallbackUnsupported  = errors.New("asynx: durable subscribers do not support WithFallback")
	ErrDeliveryParked       = errors.New("asynx: durable delivery parked")
	ErrForgetInProgress     = errors.New("asynx: aggregate is being forgotten")
	ErrSendWaitFromHandler  = errors.New("asynx: a handler cannot SendWait to the aggregate it is handling")
	ErrForgetFromHandler    = errors.New("asynx: a handler cannot forget the aggregate it is handling")
)

// SubscriberError is the failure of one durable subscriber. The delivery error
// handler and the dead-letter handler receive their errors wrapped in it, so a
// handler can tell which subscriber failed (errors.As) while errors.Is still
// sees the cause. When several subscribers fail for one event the dead-letter
// handler receives them all, joined.
type SubscriberError struct {
	// Subscriber names the subscriber: its pattern and registration order, such
	// as "OrderUpdated#1".
	Subscriber string
	Err        error
}

func (e *SubscriberError) Error() string {
	return fmt.Sprintf("subscriber %s: %v", e.Subscriber, e.Err)
}

func (e *SubscriberError) Unwrap() error { return e.Err }
