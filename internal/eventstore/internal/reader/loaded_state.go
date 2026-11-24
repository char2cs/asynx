package reader

type loadedState[T any] struct {
	state       T
	version     int64
	lastEventID string
	upcasted    bool
}
