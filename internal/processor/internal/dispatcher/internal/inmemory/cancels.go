package inmemory

import (
	"context"
	"sync"
)

type cancels struct {
	mu     sync.Mutex
	live   map[*liveCancel]struct{}
	closed bool
}

func newCancels() *cancels {
	return &cancels{live: make(map[*liveCancel]struct{})}
}

func (c *cancels) add(
	live *liveCancel,
) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		live.cancel()
		return
	}

	c.live[live] = struct{}{}
	c.mu.Unlock()
}

func (c *cancels) remove(
	live *liveCancel,
) {
	c.mu.Lock()
	delete(
		c.live,
		live,
	)
	c.mu.Unlock()
}

func (c *cancels) cancelAll() {
	c.mu.Lock()
	c.closed = true
	pending := make(
		[]context.CancelFunc,
		0,
		len(c.live),
	)
	for live := range c.live {
		pending = append(
			pending,
			live.cancel,
		)
	}

	clear(c.live)
	c.mu.Unlock()
	for _, cancel := range pending {
		cancel()
	}
}
