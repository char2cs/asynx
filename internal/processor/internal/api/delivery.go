package api

// Delivery is what an operator needs to manage durable delivery: Inspector to
// see which aggregates are parked and why, and Recoverer to redeliver once the
// cause is fixed. They are composed because looking and acting are used
// together: Paused tells which aggregates to Resume.
type Delivery interface {
	Inspector
	Recoverer
}
