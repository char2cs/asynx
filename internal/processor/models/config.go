package models

import (
	"context"

	"github.com/char2cs/asynx/v2/models/dispatcher"
	"github.com/char2cs/asynx/v2/models/event"
	"github.com/char2cs/asynx/v2/models/handler"
)

// Config holds the settings a processor is built with. The zero value selects
// the defaults.
type Config[T any] struct {
	Shards               int
	QueueDepth           int
	WorkersPerShard      int
	PublishErrorHandler  handler.PublishError[T]
	SyncProjections      []SyncProjection[T]
	DurableMatch         func(eventName string) bool
	SnapshotErrorHandler func(context.Context, event.Event[T], error)
	Dispatcher           dispatcher.Config[T]
	DispatcherFactory    dispatcher.Factory[T]
}
