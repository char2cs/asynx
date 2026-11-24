package contracts

import (
	"github.com/char2cs/asynx/v2/models/dispatch"
)

// JobSettlement is how a job reports that its handlers have run, or never
// will.
type JobSettlement interface {
	// Ticket returns the ticket that observes the job's progress.
	Ticket() dispatch.Ticket

	// MarkSettled records that the handlers have run at least once. Only the
	// first settle of a job takes effect.
	MarkSettled()

	// MarkUnrun settles a job whose handlers never ran and records why. It has
	// no effect on a job that already settled.
	MarkUnrun(
		err error,
	)
}
