package asynx

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/firsterror"
	"github.com/char2cs/asynx/internal/processor"
	procmodels "github.com/char2cs/asynx/internal/processor/models"
	"github.com/char2cs/asynx/models"
	"maps"
	"time"
)

// Build requires WithEventStore and WithSnapshotStore; all other options have
// defaults.
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

	instance := &asynxImpl[T]{
		proc: proc,
		es:   es,
		bus:  activeBus,
	}

	err = b.finish(
		instance,
		proc,
	)
	if err != nil {
		return nil,
			withCleanup(
				err,
				shutdownProcessor(proc),
			)
	}

	return instance,
		nil
}

func (b *Builder[T]) newProcessor(
	es eventstore.EventStore[T],
	activeBus models.Bus[T],
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
		return models.ErrMissingEventStore
	}

	if b.snapshotStore == nil {
		return models.ErrMissingSnapshotStore
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
	)
}

func (b *Builder[T]) activeBus() models.Bus[T] {
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
	durableSubs []models.DurableSub[T],
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
	instance *asynxImpl[T],
	proc processor.Processor[T],
) error {
	register := func(
		fn models.ForgetHandler[T],
	) error {
		_, err := instance.OnForget(fn)
		return err
	}

	err := firsterror.Of(
		b.forgetHandlers,
		register,
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
	activeBus models.Bus[T],
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
