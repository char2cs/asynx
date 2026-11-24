package aggregates

const noPark = 0

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

func (r *record[J]) isHead(
	job J,
) bool {
	if len(r.jobs) == 0 {
		return false
	}

	return any(r.jobs[0]) == any(job)
}
