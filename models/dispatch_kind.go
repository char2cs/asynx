package models

// DispatchKind says what a DispatchRequest carries, which decides who receives
// it.
//
// DispatchEvent is a freshly committed event: the live subscribers (the bus)
// receive it, and the durable subscribers whose pattern matches its name do
// when the request carries an Ack.
//
// DispatchRedelivery is an event read back from the delivery log by Recover or
// ResumeDeliveries. Only the durable subscribers receive it: live subscribers
// are fire-and-forget, they either saw the event or never will, and a replay
// would reorder their stream. A redelivered event named ForgetEventName is a
// deletion marker and reaches every durable subscriber, as the original did.
//
// DispatchTombstone is the deletion marker Forget writes. The live subscribers
// receive it, and so does every durable subscriber, whatever its pattern.
type DispatchKind int

const (
	// DispatchEvent marks a freshly committed event. It is the zero value.
	DispatchEvent DispatchKind = iota
	// DispatchRedelivery marks an event redelivered from the delivery log to
	// the durable subscribers only.
	DispatchRedelivery
	// DispatchTombstone marks the deletion marker written by Forget, delivered
	// to every durable subscriber.
	DispatchTombstone
)
