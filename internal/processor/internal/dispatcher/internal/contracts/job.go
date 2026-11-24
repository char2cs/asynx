package contracts

// Job is one event on its way through the dispatcher.
//
// A job settles once, when its handlers have run at least once or it is
// declared unrun, and completes once, when its delivery is over. Both
// transitions are idempotent and safe for concurrent use.
type Job[T any] interface {
	JobSubject[T]
	JobMode
	JobProgress
}
