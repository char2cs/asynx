package models

import "errors"

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
)
