package contracts

// LiftOutcome is what Lift writes for its caller, read from the fields once
// Lift has returned.
type LiftOutcome struct {
	// Lifted is true when the park of the requested generation was in force
	// and has been lifted; false when it was gone or had changed generation,
	// in which case Undelivered is nil.
	Lifted bool
	// Undelivered holds, in ascending order, the versions the lifted park left
	// undelivered. The registry has forgotten them.
	Undelivered []int64
}
