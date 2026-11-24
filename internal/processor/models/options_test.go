package models_test

import (
	"context"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/processor/models"
	"github.com/char2cs/asynx/models/dispatcher"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/subscription"
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

type callLog struct {
	calls []string
}

func (l *callLog) onError(
	context.Context,
	event.Event[int],
	error,
	int,
) {
	l.calls = append(
		l.calls,
		"error",
	)
}

func (l *callLog) onPanic(
	context.Context,
	event.Event[int],
	any,
) {
	l.calls = append(
		l.calls,
		"panic",
	)
}

func keepsDeadLetters(
	context.Context,
	event.Event[int],
	error,
) error {
	return nil
}

func matchesEveryEvent(
	string,
) bool {
	return true
}

func ignoresEventError(
	context.Context,
	event.Event[int],
	error,
) {
}

func buildsNoDispatcher(
	dispatcher.Config[int],
) (dispatcher.Dispatcher[int], error) {
	return nil,
		nil
}

func TestDelivery_HandlersAndLimitsReachTheDispatcherConfig(
	t *testing.T,
) {
	calls := &callLog{}
	cfg := apply(
		models.WithDurableSubscribers([]subscription.Durable[int]{{}}),
		models.WithDeliveryErrorHandler(calls.onError),
		models.WithDeliveryPanicHandler(calls.onPanic),
		models.WithDeadLetterHandler(keepsDeadLetters),
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
	cfg := apply(
		models.WithSyncProjections([]models.SyncProjection[int]{{}}),
		models.WithDurableDelivery[int](matchesEveryEvent),
		models.WithSnapshotErrorHandler(ignoresEventError),
		models.WithPublishErrorHandler[int](ignoresEventError),
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

	cfg = apply(models.WithDispatcherFactory[int](buildsNoDispatcher))
	if cfg.DispatcherFactory == nil {
		t.Fatal("a factory must be recorded")
	}
}
