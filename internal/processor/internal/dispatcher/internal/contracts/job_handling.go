package contracts

import "context"

// JobHandling is what handlers and acknowledgement need from a job.
type JobHandling interface {
	// Context returns the context handlers of this job run with. Completing
	// the job cancels it.
	Context() context.Context

	// SkipLive reports whether the live subscribers are kept out of the job,
	// as in a redelivery.
	SkipLive() bool

	// Ack returns the function to call once every durable subscriber has
	// succeeded, or nil when the job is live only.
	Ack() func(context.Context) error
}
