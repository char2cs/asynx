package main

import (
	"context"
	"testing"
)

func TestRun_Success(t *testing.T) {
	ctx := context.Background()
	err := run(ctx)
	if err != nil {
		t.Fatalf("run(ctx) = %v, want nil", err)
	}
}

func TestRun_WithCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Should handle cancelled context gracefully
	_ = run(ctx)
}
