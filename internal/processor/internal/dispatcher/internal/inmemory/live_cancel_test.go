package inmemory

import (
	"context"
	"testing"
)

func TestLiveCancel_ReleaseDeregistersAndCancels(
	t *testing.T,
) {
	c := newCancels()
	ctx, cancel := context.WithCancel(context.Background())
	live := newLiveCancel(
		c,
		cancel,
	)
	c.add(live)

	live.release()

	if ctx.Err() == nil {
		t.Fatal("release did not cancel the context")
	}

	if len(c.live) != 0 {
		t.Fatalf(
			"live = %d, want 0",
			len(c.live),
		)
	}
}
