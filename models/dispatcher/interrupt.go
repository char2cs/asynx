package dispatcher

// Interrupt makes failing durable deliveries give up retrying.
type Interrupt interface {
	// Interrupt makes failing durable deliveries give up retrying, leaving
	// them pending, without closing the dispatcher. The aggregate of a
	// delivery that carries an Ack is parked; one without an Ack is not. It
	// is called at the start of shutdown so callers waiting on a retrying
	// event are released.
	Interrupt()

	// Interrupted is closed once Interrupt or Close has been called: from then
	// on failing durable deliveries give up instead of retrying.
	Interrupted() <-chan struct{}
}
