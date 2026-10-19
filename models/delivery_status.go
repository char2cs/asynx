package models

// DeliveryStatus is what Dispatcher.Inspect reports about one aggregate's
// durable delivery: whether it is parked, which park it is, why it was parked,
// and which events the park left undelivered. The zero value describes an
// aggregate that is not parked, including one the dispatcher knows nothing of.
type DeliveryStatus struct {
	// Parked is true while the aggregate's durable delivery is halted, until
	// ResumeUndelivered lifts the park.
	Parked bool
	// Generation identifies the park, as Parks lists it. It changes each time a
	// park is imposed, so a park that is lifted and imposed again has a new
	// one. It is zero when the aggregate is not parked.
	Generation uint64
	// Cause is the error that parked the aggregate: ErrDeliveryParked wrapping
	// the last handler error and the attempt count when the attempts ran out,
	// the ack error when Ack failed, ErrDispatcherClosed on Interrupt, or the
	// error passed to Park. Parking an already parked aggregate keeps the first
	// non-nil cause. It is nil when the aggregate is not parked, and may be nil
	// for a park imposed without a cause.
	Cause error
	// Undelivered lists, in ascending order, the versions the park left
	// undelivered: the events no handler took, which a redelivery must cover.
	// It is nil when the aggregate is not parked.
	Undelivered []int64
}
