package inmemory

import (
	"context"
	"sync"
	"testing"
)

func TestCancels_CancelAllCancelsLiveContexts(
	t *testing.T,
) {
	c := newCancels()
	ctx, cancel := context.WithCancel(context.Background())
	release := c.add(cancel)

	c.cancelAll()

	if ctx.Err() == nil {
		t.Fatal("live context not cancelled by cancelAll")
	}

	release()
	release()
}

func TestCancels_AddAfterCancelAllIsAlreadyCancelled(
	t *testing.T,
) {
	c := newCancels()
	c.cancelAll()
	ctx, cancel := context.WithCancel(context.Background())

	release := c.add(cancel)

	if ctx.Err() == nil {
		t.Fatal("context added after cancelAll is not cancelled")
	}

	release()
	if len(c.live) != 0 {
		t.Fatalf(
			"live = %d, want 0",
			len(c.live),
		)
	}
}

func TestCancels_ReleaseDeregistersAndCancels(
	t *testing.T,
) {
	c := newCancels()
	ctx, cancel := context.WithCancel(context.Background())
	release := c.add(cancel)

	release()

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

func addAndRelease(
	t *testing.T,
	c *cancels,
) {
	ctx, cancel := context.WithCancel(context.Background())
	release := c.add(cancel)
	release()
	if ctx.Err() == nil {
		t.Error("context not cancelled after release")
	}
}

func spawnAddAndRelease(
	t *testing.T,
	c *cancels,
	wg *sync.WaitGroup,
) {
	wg.Go(func() {
		addAndRelease(
			t,
			c,
		)
	})
}

func TestCancels_ConcurrentAddAndCancelAll(
	t *testing.T,
) {
	c := newCancels()
	var wg sync.WaitGroup
	for range 50 {
		spawnAddAndRelease(
			t,
			c,
			&wg,
		)
		wg.Go(c.cancelAll)
	}

	wg.Wait()
}
