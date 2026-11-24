package failure

import (
	"errors"
)

// The errors below are sentinels. asynx and the stores often return them
// wrapped with detail (fmt.Errorf with %w, or joined with a cause), so compare
// with errors.Is, never with ==.
var (
	// ErrNotFound is returned when an aggregate has no events: a read of an
	// aggregate that was never created, or that was forgotten.
	ErrNotFound = errors.New("asynx: aggregate not found")
	// ErrValidation is the conventional error of a command that fails
	// validation. A command's Validate returns it (or an error wrapping it) to
	// reject the command; asynx also returns it from Forget for an aggregate
	// that does not exist. Nothing was written.
	ErrValidation = errors.New("asynx: validation failed")
	// ErrPipelineFailed is returned when the command pipeline could not commit
	// the event: a lost (streamID, version) race, which stores report by
	// wrapping it from Append, or a failure of the transaction around the
	// append, such as a synchronous projection's error. The event never
	// existed. A caller that retries must re-read the aggregate first, because
	// its state may have moved.
	ErrPipelineFailed = errors.New("asynx: pipeline failed")
	// ErrQueueFull is returned by a send that was refused because its
	// aggregate's shard queue has no room for another request. The command was
	// not run and may be retried later.
	ErrQueueFull = errors.New("asynx: queue full")
	// ErrShuttingDown is returned by any work submitted after shutdown began,
	// and to a Forget cut short by it. The work was not run.
	ErrShuttingDown = errors.New("asynx: shutting down")
	// ErrAlreadyShuttingDown is returned by a Shutdown called while another
	// Shutdown is still running. The first call keeps going.
	ErrAlreadyShuttingDown = errors.New("asynx: already shutting down")
	// ErrContextCancelled is returned by a send whose context ended: before
	// the command ran it did not run, after it started the command still
	// commits and the send still reports this error.
	ErrContextCancelled = errors.New("asynx: context cancelled")
	// ErrBusClosed is returned by Subscribe, Publish and PublishSync when the
	// bus has been closed. Nothing was registered or started.
	ErrBusClosed = errors.New("asynx: bus closed")
	// ErrNilHandler is returned when a nil handler is supplied to a
	// subscription. Nothing was registered.
	ErrNilHandler = errors.New("asynx: handler is nil")
	// ErrEmptyPattern is returned when a subscription pattern is empty.
	// Nothing was registered.
	ErrEmptyPattern = errors.New("asynx: pattern is empty")
	// ErrMissingEventStore is returned by Build when no event store was
	// configured. An instance cannot exist without one.
	ErrMissingEventStore = errors.New("asynx: event store is required")
	// ErrMissingSnapshotStore is returned by Build when no snapshot store was
	// configured. An instance cannot exist without one.
	ErrMissingSnapshotStore = errors.New("asynx: snapshot store is required")
	// ErrForgetFailed is returned when forgetting an aggregate failed after it
	// began, wrapping the cause. The aggregate keeps its tombstone and stays
	// closed to commands; calling Forget again resumes the erase.
	ErrForgetFailed = errors.New("asynx: forget failed")
	// ErrDispatcherClosed is returned when the dispatcher is used after Close,
	// and is the cause recorded when Interrupt parks an aggregate. Pending
	// deliveries stay in the delivery log for the next start.
	ErrDispatcherClosed = errors.New("asynx: dispatcher closed")
	// ErrDuplicateReceipt is returned, wrapped, by storage.TxStreamWriter.PutReceipt
	// when a receipt for the command already exists. The existing receipt is
	// unchanged.
	ErrDuplicateReceipt = errors.New("asynx: duplicate command receipt")
	// ErrSyncProjection is returned when a synchronous projection failed
	// inside the command's transaction: the transaction rolled back and the
	// command's event was not written.
	ErrSyncProjection = errors.New("asynx: sync projection failed")
	// ErrHandlerPanic is the cause of a delivery attempt in which a durable
	// handler panicked. It reaches the delivery error handler wrapped in a
	// SubscriberError.
	ErrHandlerPanic = errors.New("asynx: durable handler panicked")
	// ErrHandlerTimeout is the cause of a delivery attempt in which a durable
	// handler exceeded its timeout. It reaches the delivery error handler
	// wrapped in a SubscriberError; the handler is still waited for.
	ErrHandlerTimeout = errors.New("asynx: durable handler timed out")
	// ErrFallbackUnsupported is returned by Build when subscription.WithFallback is used on a
	// durable subscriber, which retries instead of falling back.
	ErrFallbackUnsupported = errors.New("asynx: durable subscribers do not support WithFallback")
	// ErrDeliveryParked is returned when an aggregate's durable delivery is
	// parked, so the event was or will be left undelivered until the park is
	// lifted. It wraps the cause of the park when there is one.
	ErrDeliveryParked = errors.New("asynx: durable delivery parked")
	// ErrForgetInProgress is returned for a command sent to an aggregate from
	// its tombstone until its erase finishes. The command was not run.
	ErrForgetInProgress = errors.New("asynx: aggregate is being forgotten")
	// ErrSendWaitFromHandler is returned when a handler SendWaits to the
	// aggregate it is handling, which would wait on the handler itself. The
	// command was not run.
	ErrSendWaitFromHandler = errors.New("asynx: a handler cannot SendWait to the aggregate it is handling")
	// ErrForgetFromHandler is returned when a handler forgets the aggregate it
	// is handling, which would wait on the handler itself. Nothing was
	// forgotten.
	ErrForgetFromHandler = errors.New("asynx: a handler cannot forget the aggregate it is handling")
)
