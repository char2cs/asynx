package contracts

// JobProgress is a job's way through settlement to completion.
type JobProgress interface {
	JobSettlement
	JobCompletion
}
