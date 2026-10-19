package models

// Ticket tracks one enqueued event.
type Ticket struct {
	// Settled closes once every handler has run at least once. A failing
	// durable handler keeps retrying after it closes.
	Settled <-chan struct{}
	// Final closes when the event's delivery is over. Err then says how it
	// ended: nil if every handler succeeded (and the ack ran), else
	// ErrDeliveryParked, ErrDispatcherClosed or an ack failure.
	Final <-chan struct{}
	// Err is valid once Final is closed.
	Err func() error
	// Unrun, once Settled is closed, reports why no handler ran at all: the
	// event was not queued, or left the queue, because its aggregate is parked.
	// It is nil when the handlers ran.
	Unrun func() error
}
