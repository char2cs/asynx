package asynx

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/eventstore/options"
	"github.com/char2cs/asynx/internal/firsterror"
	"github.com/char2cs/asynx/internal/processor"
	procmodels "github.com/char2cs/asynx/internal/processor/models"
	"github.com/char2cs/asynx/models/failure"
	"github.com/char2cs/asynx/models/pubsub"
	"github.com/char2cs/asynx/models/subscription"
	"maps"
	"time"
)

// Build assembles the instance from the configuration and returns it, ready to
// use. WithEventStore and WithSnapshotStore are required (it returns
// failure.ErrMissingEventStore or failure.ErrMissingSnapshotStore without them);
// all other options have defaults.
//
// Build validates the durable subscribers and synchronous projections: an empty
// pattern (failure.ErrEmptyPattern), a nil handler (failure.ErrNilHandler), a
// pattern that is not a valid regular expression, or WithFallback on a durable
// subscriber (failure.ErrFallbackUnsupported) fails it. When the instance has
// durable subscribers Build also queues the deliveries a crash left
// unacknowledged for redelivery, in per-aggregate order, and returns without
// waiting for the handlers.
//
// A failed Build leaves nothing running: the bus and the processor it created
// are shut down, within 30 seconds, and a failure of that cleanup is joined
// to the returned error (see errors.Is and errors.As), never dropped. On
// failure the Asynx is nil.
func (b *Builder[T]) Build() (Asynx[T], error) {
	err := b.requireStores()
	if err != nil {
		return nil,
			err
	}

	activeBus := b.activeBus()
	es := b.newEventStore()

	proc, err := b.newProcessor(
		es,
		activeBus,
	)
	if err != nil {
		return nil,
			err
	}

	built := &instance[T]{
		proc: proc,
		es:   es,
		bus:  activeBus,

		onListenerError: b.listenerErrHandler,
	}

	err = b.finish(
		built,
		proc,
	)
	if err != nil {
		return nil,
			withCleanup(
				err,
				shutdownProcessor(proc),
			)
	}

	return built,
		nil
}

func (b *Builder[T]) newProcessor(
	es eventstore.EventStore[T],
	activeBus pubsub.Bus[T],
) (processor.Processor[T], error) {
	procOpts, err := b.processorOpts()
	if err != nil {
		return nil,
			err
	}

	proc, err := processor.New(
		es,
		activeBus,
		procOpts...,
	)
	if err != nil {
		return nil,
			withCleanup(
				err,
				closeAbandonedBus(activeBus),
			)
	}

	return proc,
		nil
}

func (b *Builder[T]) requireStores() error {
	if b.eventStore == nil {
		return failure.ErrMissingEventStore
	}

	if b.snapshotStore == nil {
		return failure.ErrMissingSnapshotStore
	}

	return nil
}

func (b *Builder[T]) newEventStore() eventstore.EventStore[T] {
	return eventstore.New[T](
		b.eventStore,
		b.snapshotStore,
		maps.Clone(b.upcasters),
		b.schemaVersion,
		b.corruptionHook,
		options.WithSnapshotErrorHandler(b.snapshotErrHandler),
	)
}

func (b *Builder[T]) activeBus() pubsub.Bus[T] {
	if b.bus != nil {
		return b.bus
	}

	if b.panicHandler != nil {
		return bus.New[T](
			bus.WithPanicHandler[T](b.panicHandler),
		)
	}

	return bus.New[T]()
}

func (b *Builder[T]) processorOpts() ([]procmodels.Opt[T], error) {
	syncs, err := b.compileSyncProjections()
	if err != nil {
		return nil,
			err
	}

	procOpts := []procmodels.Opt[T]{
		procmodels.WithShards[T](b.shardingOpts.Shards),
		procmodels.WithQueueDepth[T](b.shardingOpts.QueueDepth),
		procmodels.WithWorkersPerShard[T](b.shardingOpts.WorkersPerShard),
	}
	if len(syncs) > 0 {
		procOpts = append(
			procOpts,
			procmodels.WithSyncProjections(syncs),
		)
	}

	durableSubs, durableMatch, err := b.compileDurable()
	if err != nil {
		return nil,
			err
	}

	procOpts = append(
		procOpts,
		b.durableOpts(
			durableSubs,
			durableMatch,
		)...,
	)

	return append(
			procOpts,
			b.handlerOpts()...,
		),
		nil
}

func (b *Builder[T]) handlerOpts() (opts []procmodels.Opt[T]) {
	if b.snapshotErrHandler != nil {
		opts = append(
			opts,
			procmodels.WithSnapshotErrorHandler(b.snapshotErrHandler),
		)
	}

	if b.publishErrorHandler != nil {
		opts = append(
			opts,
			procmodels.WithPublishErrorHandler[T](b.publishErrorHandler),
		)
	}

	if b.dispatcherFactory != nil {
		opts = append(
			opts,
			procmodels.WithDispatcherFactory(b.dispatcherFactory),
		)
	}

	return opts
}

func (b *Builder[T]) durableOpts(
	durableSubs []subscription.Durable[T],
	durableMatch func(string) bool,
) []procmodels.Opt[T] {
	if durableMatch == nil {
		return nil
	}

	opts := []procmodels.Opt[T]{
		procmodels.WithDurableDelivery[T](durableMatch),
		procmodels.WithDurableSubscribers(durableSubs),
	}
	if b.deliveryErrHandler != nil {
		opts = append(
			opts,
			procmodels.WithDeliveryErrorHandler(b.deliveryErrHandler),
		)
	}

	if b.panicHandler != nil {
		opts = append(
			opts,
			procmodels.WithDeliveryPanicHandler(b.panicHandler),
		)
	}

	return append(
		opts,
		b.retryOpts()...,
	)
}

func (b *Builder[T]) retryOpts() []procmodels.Opt[T] {
	opts := []procmodels.Opt[T]{
		procmodels.WithDeliveryRetry[T](
			b.retryInitial,
			b.retryMax,
		),
	}
	if b.maxAttempts > 0 {
		opts = append(
			opts,
			procmodels.WithDeliveryMaxAttempts[T](b.maxAttempts),
		)
	}

	if b.deadLetter != nil {
		opts = append(
			opts,
			procmodels.WithDeadLetterHandler(b.deadLetter),
		)
	}

	return opts
}

func (b *Builder[T]) finish(
	built *instance[T],
	proc processor.Processor[T],
) error {
	err := firsterror.Of(
		b.forgetHandlers,
		built.subscribeForget,
	)
	if err != nil {
		return err
	}

	if len(b.subscribers) == 0 {
		return nil
	}

	return proc.Recover(
		context.Background(),
		recoveryBatch,
	)
}

func withCleanup(
	err error,
	cleanupErr error,
) error {
	if cleanupErr == nil {
		return err
	}

	return errors.Join(
		err,
		cleanupErr,
	)
}

func shutdownProcessor[T any](
	proc processor.Processor[T],
) error {
	sctx, cancel := context.WithTimeout(
		context.Background(),
		failedBuildShutdown,
	)
	defer cancel()

	return proc.Shutdown(sctx)
}

func closeAbandonedBus[T any](
	activeBus pubsub.Bus[T],
) error {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		failedBuildShutdown,
	)
	defer cancel()

	return activeBus.Close(ctx)
}

const failedBuildShutdown = 30 * time.Second

const recoveryBatch = 100
