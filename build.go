package asynx

import (
	"context"
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
	if b.eventStore == nil {
		return nil,
			models.ErrMissingEventStore
	}

	if b.snapshotStore == nil {
		return nil,
			models.ErrMissingSnapshotStore
	}

	activeBus := b.activeBus()

	es := eventstore.New[T](
		b.eventStore,
		b.snapshotStore,
		maps.Clone(b.upcasters),
		b.schemaVersion,
		b.corruptionHook,
	)

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
		closeAbandonedBus(activeBus)
		return nil,
			err
	}

	instance := &asynxImpl[T]{
		proc: proc,
		es:   es,
		bus:  activeBus,
	}

	built := false
	defer shutdownUnlessBuilt(
		proc,
		&built,
	)

	err = b.finish(
		instance,
		proc,
	)
	if err != nil {
		return nil,
			err
	}

	built = true
	return instance,
		nil
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
	if b.snapshotErrHandler != nil {
		procOpts = append(
			procOpts,
			procmodels.WithSnapshotErrorHandler(b.snapshotErrHandler),
		)
	}

	if b.publishErrorHandler != nil {
		procOpts = append(
			procOpts,
			procmodels.WithPublishErrorHandler[T](b.publishErrorHandler),
		)
	}

	if b.dispatcherFactory != nil {
		procOpts = append(
			procOpts,
			procmodels.WithDispatcherFactory(b.dispatcherFactory),
		)
	}

	return procOpts,
		nil
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

	opts = append(
		opts,
		procmodels.WithDeliveryRetry[T](
			b.retryInitial,
			b.retryMax,
		),
	)
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
	err := firsterror.Of(
		b.forgetHandlers,
		func(
			fn models.ForgetHandler[T],
		) error {
			_, err := instance.OnForget(fn)
			return err
		},
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

func shutdownUnlessBuilt[T any](
	proc processor.Processor[T],
	built *bool,
) {
	if *built {
		return
	}

	sctx, cancel := context.WithTimeout(
		context.Background(),
		failedBuildShutdown,
	)
	defer cancel()
	_ = proc.Shutdown(sctx)
}

func closeAbandonedBus[T any](
	activeBus models.Bus[T],
) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		failedBuildShutdown,
	)
	defer cancel()
	_ = activeBus.Close(ctx)
}

const failedBuildShutdown = 30 * time.Second

const recoveryBatch = 100
