package models_test

import (
	"context"
	"testing"
	"time"

	"github.com/char2cs/asynx/models"
)

func TestSubscriptionOptions_SetTheirConfig(t *testing.T) {
	called := false
	fallback := func(context.Context, models.Event[int]) { called = true }

	var cfg models.SubscriptionConfig[int]
	models.WithFallback[int](fallback)(&cfg)
	models.WithHandlerTimeout[int](3 * time.Second)(&cfg)

	if cfg.Timeout != 3*time.Second {
		t.Errorf("Timeout = %v, want 3s", cfg.Timeout)
	}
	cfg.Fallback(context.Background(), models.Event[int]{})
	if !called {
		t.Error("Fallback is not the registered handler")
	}
}
