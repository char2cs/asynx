package race

import (
	"sync"
)

type lanes struct {
	start    sync.WaitGroup
	finished sync.WaitGroup
	results  []error
}

// All runs run(0) through run(n-1), each on its own goroutine, and returns
// their errors indexed by lane once every one has finished. The goroutines are
// held back until all of them exist and then released together, so the calls
// contend as hard as the scheduler allows instead of running one after
// another. run must be safe for concurrent use. A panic in a lane is not
// recovered.
func All(
	n int,
	run func(i int) error,
) []error {
	l := &lanes{results: make(
		[]error,
		n,
	)}
	l.start.Add(1)
	for i := range n {
		l.finished.Add(1)
		go l.lane(
			i,
			run,
		)
	}

	l.start.Done()
	l.finished.Wait()
	return l.results
}

func (l *lanes) lane(
	i int,
	run func(i int) error,
) {
	defer l.finished.Done()
	l.start.Wait()
	l.results[i] = run(i)
}
