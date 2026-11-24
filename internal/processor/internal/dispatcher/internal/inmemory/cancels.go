package inmemory

import (
	"context"
	"sync"
)

type cancels struct {
	mu     sync.Mutex
	next   uint64
	live   map[uint64]context.CancelFunc
	closed bool
}

func newCancels() *cancels {
	return &cancels{live: make(map[uint64]context.CancelFunc)}
}

func (c *cancels) add(
	cancel context.CancelFunc,
) (release context.CancelFunc) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		cancel()
		return cancel
	}

	c.next++
	id := c.next
	c.live[id] = cancel
	c.mu.Unlock()
	return func() {
		c.remove(id)
		cancel()
	}
}

func (c *cancels) remove(
	id uint64,
) {
	c.mu.Lock()
	delete(
		c.live,
		id,
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
	for _, cancel := range c.live {
		pending = append(
			pending,
			cancel,
		)
	}

	clear(c.live)
	c.mu.Unlock()
	for _, cancel := range pending {
		cancel()
	}
}
