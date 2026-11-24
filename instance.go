package asynx

import (
	"context"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/processor"
	"github.com/char2cs/asynx/models"
)

type instance[T any] struct {
	proc processor.Processor[T]
	es   eventstore.EventStore[T]
	bus  models.Bus[T]

	onListenerError func(error)
}

// Send validates and persists the command, then returns the resulting event.
// It delegates to the processor; see CommandSender.Send for the contract.
func (i *instance[T]) Send(
	ctx context.Context,
	cmd models.Command[T],
) (models.Event[T], error) {
	return i.proc.Send(
		ctx,
		cmd,
	)
}

// SendWait behaves like Send and also waits for the matching projection
// handlers. See CommandSender.SendWait for the contract.
func (i *instance[T]) SendWait(
	ctx context.Context,
	cmd models.Command[T],
) (models.Event[T], error) {
	return i.proc.SendWait(
		ctx,
		cmd,
	)
}

// Shutdown stops the instance, waiting for in-flight work until ctx is done.
// See Lifecycle.Shutdown for the contract.
func (i *instance[T]) Shutdown(
	ctx context.Context,
) error {
	return i.proc.Shutdown(ctx)
}

// Get returns the current state of the aggregate, read through the event
// store. See AggregateReader.Get for the contract.
func (i *instance[T]) Get(
	ctx context.Context,
	aggregateID string,
) (T, error) {
	return i.es.Get(
		ctx,
		aggregateID,
	)
}

// Exists reports whether the aggregate has any event. See
// AggregateReader.Exists for the contract.
func (i *instance[T]) Exists(
	ctx context.Context,
	aggregateID string,
) (bool, error) {
	return i.es.Exists(
		ctx,
		aggregateID,
	)
}

// Preload rebuilds the aggregate so that a snapshot is available to later
// reads. See AggregateReader.Preload for the contract.
func (i *instance[T]) Preload(
	ctx context.Context,
	aggregateID string,
) error {
	return i.es.Preload(
		ctx,
		aggregateID,
	)
}

// Subscribe registers a projection handler on the bus for events matching
// pattern. See SubscriptionManager.Subscribe for the contract.
func (i *instance[T]) Subscribe(
	pattern string,
	handler models.ProjectionHandler[T],
	opts ...models.SubscriptionOpt[T],
) (string, error) {
	return i.bus.Subscribe(
		pattern,
		handler,
		opts...,
	)
}

// Unsubscribe removes the bus subscription with the given ID. See
// SubscriptionManager.Unsubscribe for the contract.
func (i *instance[T]) Unsubscribe(
	id string,
) error {
	return i.bus.Unsubscribe(id)
}

// Listen opens a channel-based subscription for events matching pattern. See
// EventListener.Listen for the contract.
func (i *instance[T]) Listen(
	pattern string,
	count int,
) (<-chan models.Event[T], func(), error) {
	l, err := i.openListener(
		pattern,
		count,
	)
	if err != nil {
		return nil,
			nil,
			err
	}

	return l.ch,
		l.release,
		nil
}

func (i *instance[T]) openListener(
	pattern string,
	count int,
) (*listener[T], error) {
	if pattern == "" {
		return nil,
			models.ErrEmptyPattern
	}

	l := newListener(
		i.bus,
		count,
		i.onListenerError,
	)
	id, err := i.bus.Subscribe(
		Topic(pattern),
		l.handle,
	)
	if err != nil {
		return nil,
			err
	}

	l.publishSubscription(id)

	return l,
		nil
}

// SubscribeWait blocks until an event matching pattern arrives or ctx is done.
// See EventListener.SubscribeWait for the contract.
func (i *instance[T]) SubscribeWait(
	ctx context.Context,
	pattern string,
) (evt models.Event[T], err error) {
	l, err := i.openListener(
		pattern,
		1,
	)
	if err != nil {
		return evt,
			err
	}

	defer func() {
		err = withCleanup(
			err,
			l.unsubscribe(),
		)
	}()

	select {
	case evt = <-l.ch:
		return evt,
			nil
	case <-ctx.Done():
		return evt,
			ctx.Err()
	}
}

// Replay feeds the events of an aggregate between two versions to fn, reading
// the event store directly. See EventListener.Replay for the contract.
func (i *instance[T]) Replay(
	ctx context.Context,
	aggregateID string,
	fromVersion int64,
	toVersion int64,
	fn models.ProjectionHandler[T],
) error {
	return i.es.Replay(
		ctx,
		aggregateID,
		fromVersion,
		toVersion,
		fn,
	)
}

// WaitPublish waits for the processor's queued publishes and then for the
// bus's running handlers. Only for use in tests; see Lifecycle.WaitPublish.
func (i *instance[T]) WaitPublish() {
	i.proc.WaitPublish()
	i.bus.WaitForHandlers()
}

// Forget tombstones the aggregate, notifies the forget handlers and durable
// subscribers, then erases it. See CommandSender.Forget for the contract.
func (i *instance[T]) Forget(
	ctx context.Context,
	aggregateID string,
) error {
	return i.proc.Forget(
		ctx,
		aggregateID,
	)
}

// PausedAggregates lists the aggregates whose durable delivery is parked. See
// DeliveryControl.PausedAggregates for the contract.
func (i *instance[T]) PausedAggregates() []string {
	return i.proc.Paused()
}

// InspectDelivery reports why an aggregate is paused. See
// DeliveryControl.InspectDelivery for the contract.
func (i *instance[T]) InspectDelivery(
	aggregateID string,
) models.DeliveryStatus {
	return i.proc.Inspect(aggregateID)
}

// ResumeDeliveries redelivers the paused aggregates' pending events to the
// durable subscribers. See
// DeliveryControl.ResumeDeliveries for the contract.
func (i *instance[T]) ResumeDeliveries(
	ctx context.Context,
) error {
	return i.proc.Resume(
		ctx,
		recoveryBatch,
	)
}

// OnForget registers fn to run when an aggregate is forgotten. The dots of the
// subscription pattern are escaped so the bus treats it as a literal, not a
// wildcard.
func (i *instance[T]) OnForget(
	fn models.ForgetHandler[T],
) (string, error) {
	return i.bus.Subscribe(
		"asynx\\.aggregate\\.forget",
		models.ProjectionHandler[T](fn),
	)
}
