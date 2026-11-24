package models_test

import (
	"context"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/processor/models"
	asynxmd "github.com/char2cs/asynx/models"
)

func apply(
	opts ...models.Opt[int],
) (cfg models.Config[int]) {
	for _, opt := range opts {
		opt(&cfg)
	}

	return cfg
}

func TestSharding_PositiveValuesApplyAndOthersAreIgnored(
	t *testing.T,
) {
	cfg := apply(
		models.WithShards[int](3),
		models.WithQueueDepth[int](4),
		models.WithWorkersPerShard[int](5),
	)
	if cfg.Shards != 3 ||
		cfg.QueueDepth != 4 ||
		cfg.WorkersPerShard != 5 {
		t.Fatalf(
			"sharding = %d/%d/%d, want 3/4/5",
			cfg.Shards,
			cfg.QueueDepth,
			cfg.WorkersPerShard,
		)
	}

	cfg = apply(
		models.WithShards[int](0),
		models.WithQueueDepth[int](-1),
		models.WithWorkersPerShard[int](0),
	)
	if cfg.Shards != 0 ||
		cfg.QueueDepth != 0 ||
		cfg.WorkersPerShard != 0 {
		t.Fatalf(
			"sharding = %d/%d/%d, want the zero values kept",
			cfg.Shards,
			cfg.QueueDepth,
			cfg.WorkersPerShard,
		)
	}
}

func TestDelivery_HandlersAndLimitsReachTheDispatcherConfig(
	t *testing.T,
) {
	var calls []string
	onError := func(
		context.Context,
		asynxmd.Event[int],
		error,
		int,
	) {
		calls = append(
			calls,
			"error",
		)
	}
	onPanic := func(
		context.Context,
		asynxmd.Event[int],
		any,
	) {
		calls = append(
			calls,
			"panic",
		)
	}
	deadLetter := func(
		context.Context,
		asynxmd.Event[int],
		error,
	) error {
		return nil
	}
	cfg := apply(
		models.WithDurableSubscribers([]asynxmd.DurableSub[int]{{}}),
		models.WithDeliveryErrorHandler(onError),
		models.WithDeliveryPanicHandler(onPanic),
		models.WithDeadLetterHandler(deadLetter),
		models.WithDeliveryMaxAttempts[int](7),
		models.WithDeliveryRetry[int](
			time.Second,
			time.Minute,
		),
	)
	if len(cfg.Dispatcher.DurableSubs) != 1 ||
		cfg.Dispatcher.OnDeliveryError == nil ||
		cfg.Dispatcher.OnPanic == nil ||
		cfg.Dispatcher.DeadLetter == nil {
		t.Fatalf(
			"dispatcher config = %+v, want the handlers set",
			cfg.Dispatcher,
		)
	}

	if cfg.Dispatcher.MaxAttempts != 7 ||
		cfg.Dispatcher.RetryInitial != time.Second ||
		cfg.Dispatcher.RetryMax != time.Minute {
		t.Fatalf(
			"dispatcher config = %+v, want the limits set",
			cfg.Dispatcher,
		)
	}
}

func TestDelivery_NonPositiveLimitsKeepTheEarlierValues(
	t *testing.T,
) {
	cfg := apply(
		models.WithDeliveryMaxAttempts[int](3),
		models.WithDeliveryMaxAttempts[int](0),
		models.WithDeliveryRetry[int](
			time.Second,
			time.Minute,
		),
		models.WithDeliveryRetry[int](
			0,
			-1,
		),
	)
	if cfg.Dispatcher.MaxAttempts != 3 ||
		cfg.Dispatcher.RetryInitial != time.Second ||
		cfg.Dispatcher.RetryMax != time.Minute {
		t.Fatalf(
			"dispatcher config = %+v, want the earlier values kept",
			cfg.Dispatcher,
		)
	}
}

func TestExecutor_SettingsAreRecorded(
	t *testing.T,
) {
	match := func(
		string,
	) bool {
		return true
	}
	onEvent := func(
		context.Context,
		asynxmd.Event[int],
		error,
	) {
	}
	cfg := apply(
		models.WithSyncProjections([]models.SyncProjection[int]{{}}),
		models.WithDurableDelivery[int](match),
		models.WithSnapshotErrorHandler(onEvent),
		models.WithPublishErrorHandler[int](onEvent),
	)
	if len(cfg.SyncProjections) != 1 ||
		cfg.DurableMatch == nil ||
		cfg.SnapshotErrorHandler == nil ||
		cfg.PublishErrorHandler == nil {
		t.Fatalf(
			"config = %+v, want every setting recorded",
			cfg,
		)
	}
}

func TestWithDispatcherFactory_IgnoresNil(
	t *testing.T,
) {
	cfg := apply(models.WithDispatcherFactory[int](nil))
	if cfg.DispatcherFactory != nil {
		t.Fatal("a nil factory must be ignored")
	}

	factory := func(
		asynxmd.DispatcherConfig[int],
	) (asynxmd.Dispatcher[int], error) {
		return nil,
			nil
	}
	cfg = apply(models.WithDispatcherFactory[int](factory))
	if cfg.DispatcherFactory == nil {
		t.Fatal("a factory must be recorded")
	}
}
