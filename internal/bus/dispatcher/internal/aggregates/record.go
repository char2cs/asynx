package aggregates

type record[J Item] struct {
	jobs        []J
	worker      bool
	wake        chan struct{}
	halt        chan struct{}
	parked      bool
	generation  uint64
	cause       error
	undelivered map[int64]error
}
