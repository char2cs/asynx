package dispatch

// Kind says what a Request carries, which decides who receives
// it.
//
// KindEvent is a freshly committed event: the live subscribers (the bus)
// receive it, and the durable subscribers whose pattern matches its name do
// when the request carries an Ack.
//
// KindRedelivery is an event read back from the delivery log by Recover or
// ResumeDeliveries. Only the durable subscribers receive it: live subscribers
// are fire-and-forget, they either saw the event or never will, and a replay
// would reorder their stream. A redelivered event named event.ForgetEventName is a
// deletion marker and reaches every durable subscriber, as the original did.
//
// KindTombstone is the deletion marker Forget writes. The live subscribers
// receive it, and so does every durable subscriber, whatever its pattern.
type Kind int

const (
	// KindEvent marks a freshly committed event. It is the zero value.
	KindEvent Kind = iota
	// KindRedelivery marks an event redelivered from the delivery log to
	// the durable subscribers only.
	KindRedelivery
	// KindTombstone marks the deletion marker written by Forget, delivered
	// to every durable subscriber.
	KindTombstone
)
