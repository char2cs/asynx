package mocks

// ParkCall is one call to Dispatcher.Park: the aggregate parked, the cause it
// was given and the undelivered versions it was given.
type ParkCall struct {
	AggregateID string
	Cause       error
	Undelivered []int64
}
