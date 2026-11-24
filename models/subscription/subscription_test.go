package subscription_test

import (
	"context"
	"testing"
	"time"

	"github.com/char2cs/asynx/v2/models/event"
	"github.com/char2cs/asynx/v2/models/subscription"
)

func TestSubscriptionOptions_SetTheirConfig(
	t *testing.T,
) {
	called := false
	fallback := func(
		context.Context,
		event.Event[int],
	) {
		called = true
	}

	var cfg subscription.Config[int]
	subscription.WithFallback[int](fallback)(&cfg)
	subscription.WithHandlerTimeout[int](3 * time.Second)(&cfg)

	if cfg.Timeout != 3*time.Second {
		t.Errorf(
			"Timeout = %v, want 3s",
			cfg.Timeout,
		)
	}

	cfg.Fallback(
		context.Background(),
		event.Event[int]{},
	)
	if !called {
		t.Error("Fallback is not the registered handler")
	}
}
