package aggregates

type record[J Item] struct {
	jobs        []J
	active      int
	tail        <-chan struct{}
	halt        chan struct{}
	parked      bool
	generation  uint64
	cause       error
	undelivered map[int64]error
}
