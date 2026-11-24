package contracts

// JobCompletion ends a job's delivery.
type JobCompletion interface {
	// Complete ends the job's delivery with err: it cancels the job's context,
	// closes the ticket's Final channel and runs the completion callback. Only
	// the first call takes effect.
	Complete(
		err error,
	)

	// OnComplete sets the callback Complete runs once, replacing any previous
	// one. Set it before the job is shared between goroutines.
	OnComplete(
		callback func(),
	)
}
