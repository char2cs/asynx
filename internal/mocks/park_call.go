package mocks

// ParkCall is one call to Dispatcher.Park: the aggregate parked and the
// undelivered versions it was given.
type ParkCall struct {
	AggregateID string
	Undelivered []int64
}
