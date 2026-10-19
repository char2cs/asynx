package processor

import (
	"testing"
	"time"
)

func TestRetryBackoff_IgnoresNonPositiveValues(
	t *testing.T,
) {
	cfg := configure(WithDeliveryRetry[int](0, -1))
	if cfg.dispatcher.RetryInitial != defaultRetryInitial || cfg.dispatcher.RetryMax != defaultRetryMax {
		t.Fatalf("backoff = %v..%v, want defaults", cfg.dispatcher.RetryInitial, cfg.dispatcher.RetryMax)
	}

	cfg = configure(WithDeliveryRetry[int](time.Second, time.Minute), WithDeliveryRetry[int](0, -1))
	if cfg.dispatcher.RetryInitial != time.Second || cfg.dispatcher.RetryMax != time.Minute {
		t.Fatalf("backoff = %v..%v, want the earlier positive values kept", cfg.dispatcher.RetryInitial, cfg.dispatcher.RetryMax)
	}
}

func TestOptions_NonPositiveBoundsAreIgnored(
	t *testing.T,
) {
	cfg := configure(WithDeliveryMaxAttempts[int](0))
	if cfg.dispatcher.MaxAttempts != defaultMaxAttempts {
		t.Fatalf("maxAttempts=%d, want the default", cfg.dispatcher.MaxAttempts)
	}

	cfg = configure(WithDeliveryMaxAttempts[int](3), WithDeliveryMaxAttempts[int](-1))
	if cfg.dispatcher.MaxAttempts != 3 {
		t.Fatalf("maxAttempts=%d, want the earlier positive value kept", cfg.dispatcher.MaxAttempts)
	}
}

func TestConfigure_PassesTheDeliverySettingsToTheDispatcherConfig(
	t *testing.T,
) {
	cfg := configure(WithDeliveryRetry[int](time.Second, time.Minute), WithDeliveryMaxAttempts[int](7))
	wrongConfig := cfg.dispatcher.RetryInitial != time.Second || cfg.dispatcher.RetryMax != time.Minute || cfg.dispatcher.MaxAttempts != 7
	if wrongConfig {
		t.Fatalf("config = %+v, want the values set", cfg.dispatcher)
	}
}
