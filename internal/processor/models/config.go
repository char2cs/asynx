package models

import (
	"context"

	asynxmd "github.com/char2cs/asynx/models"
)

// Config holds the settings a processor is built with. The zero value selects
// the defaults.
type Config[T any] struct {
	Shards               int
	QueueDepth           int
	WorkersPerShard      int
	PublishErrorHandler  asynxmd.PublishErrorHandler[T]
	SyncProjections      []SyncProjection[T]
	DurableMatch         func(eventName string) bool
	SnapshotErrorHandler func(context.Context, asynxmd.Event[T], error)
	Dispatcher           asynxmd.DispatcherConfig[T]
	DispatcherFactory    asynxmd.DispatcherFactory[T]
}
