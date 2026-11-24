package mockbuses

import (
	"context"
	"testing"
)

func closeOnce(
	t *testing.T,
	b *ClosingBus[fakeOrder],
) {
	t.Helper()

	err := b.Close(context.Background())
	if err != nil {
		t.Fatalf(
			"Close = %v, want nil",
			err,
		)
	}
}

func TestClosingBus_CountsEveryClose(
	t *testing.T,
) {
	b := &ClosingBus[fakeOrder]{}
	for range 2 {
		closeOnce(
			t,
			b,
		)
	}

	if got := b.Closes.Load(); got != 2 {
		t.Fatalf(
			"Closes = %d, want 2",
			got,
		)
	}
}
