package contracts

// JobSubject is the event a job delivers and what handlers receive with it.
type JobSubject[T any] interface {
	JobIdentity[T]
	JobHandling
}
