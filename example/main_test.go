package main

import (
	"context"
	"errors"
	"testing"

	"github.com/char2cs/asynx/v2/models/failure"
)

func TestRun_Success(
	t *testing.T,
) {
	ctx := context.Background()
	err := run(ctx)
	if err != nil {
		t.Fatalf(
			"run(ctx) = %v, want nil",
			err,
		)
	}
}

func TestRun_WithCancelledContext(
	t *testing.T,
) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := run(ctx)
	if !errors.Is(
		err,
		failure.ErrContextCancelled,
	) {
		t.Fatalf(
			"run(cancelled ctx) = %v, want it to report models.ErrContextCancelled",
			err,
		)
	}
}
