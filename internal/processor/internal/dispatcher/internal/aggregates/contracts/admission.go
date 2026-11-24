package contracts

// Admission is what Enqueue writes for its caller: what became of the job,
// read from the fields once Enqueue has returned.
type Admission struct {
	// Parked is true when the job was refused because its aggregate is parked:
	// it was not queued, its version is recorded as undelivered, and it has
	// been marked unrun and completed with ErrDeliveryParked. Turn and Done
	// are nil then.
	Parked bool
	// Turn is closed once the aggregate's previous job has finished, which is
	// when this job may begin. It is closed already for the first job of an
	// idle aggregate.
	Turn <-chan struct{}
	// Done is the channel the job hands its turn on with, through
	// Winddown.Finish: the next job of the aggregate waits for it.
	Done chan<- struct{}
	// Err is ErrDispatcherClosed when the registry is closed, in which case
	// nothing else is set; nil otherwise.
	Err error
}
