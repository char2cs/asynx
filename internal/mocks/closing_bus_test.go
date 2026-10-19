package mocks

import (
	"context"
	"testing"
)

func TestClosingBus_CountsEveryClose(t *testing.T) {
	b := &ClosingBus{}
	for range 2 {
		if err := b.Close(context.Background()); err != nil {
			t.Fatalf("Close = %v, want nil", err)
		}
	}
	if got := b.Closes.Load(); got != 2 {
		t.Fatalf("Closes = %d, want 2", got)
	}
}
