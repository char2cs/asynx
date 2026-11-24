package storetest

import (
	"errors"
	"github.com/char2cs/asynx/models"
	"maps"
	"testing"
)

// Factory returns a fresh, empty store for one subtest. The suite calls it
// once per scenario and never shares a store between scenarios, so a
// Factory may hand out a store backed by a new temporary database, or by the
// same one if it is emptied first.
type Factory func(
	t *testing.T,
) models.Store

var errBoom = errors.New("storetest: boom")

// Run executes the full conformance suite for models.Store implementations
// against stores built by newStore. A store that passes it keeps the
// guarantees asynx builds on: events are written atomically with their
// receipts and deliveries, a (stream, version) pair is stored once, reads
// return streams in version order, and every operation honours its context.
// Call it from a test in the package that provides the store:
//
//	func TestConformance(t *testing.T) {
//		storetest.Run(t, func(t *testing.T) models.Store { return newMyStore(t) })
//	}
//
// Each scenario runs as a subtest named after the guarantee it checks, against
// its own empty store.
//
// The scenarios cover, by guarantee:
//
// Streams. AppendCommitsAndReadFromReturnsInOrder and ReadRangeLimitsCount
// check that committed blobs read back in order and that ReadRange cuts the
// window to its count. AppendsOutOfVersionOrderReadInOrder checks that reads
// return a stream by ascending version however it was appended.
// ReadsOfUnknownStreamAreEmpty checks that a missing stream is empty, not an
// error. DeleteStreamIsIdempotentAndScoped checks that DeleteStream empties
// only its stream, tolerates a missing one and leaves the stream usable again.
// CommitInsideAtomicallyIsVisibleAfterwards checks that separate commits
// accumulate.
//
// Transactions. DuplicateVersionConflictsAndRollsBack checks that a second
// write of one (stream, version) fails with models.ErrPipelineFailed and takes
// the rest of its transaction with it. ErrorRollsBackEverything checks that an
// error from the callback undoes appends, receipts and deliveries alike and is
// returned to the caller. RollbackRestoresDroppedDeliveries checks that a
// rolled-back DropDeliveries brings the entries back. NativeIsCallable checks
// that Tx.Native can be called inside a transaction.
// CancelledContextFailsEveryOperation checks that a context that is already
// done fails every method and changes nothing. ReadsDuringAnOpenTransactionDoNotFail
// checks that a read made while a transaction is open neither fails nor
// deadlocks it, and that the committed write is visible afterwards.
//
// Receipts and deliveries. ReceiptsAreStoredAndDuplicatesRejected checks that
// a receipt reads back as stored, that a second receipt for one command fails
// with models.ErrDuplicateReceipt, and that an unknown command has none.
// DeliveriesKeepEnqueueOrderAndAckRemoves, PendingDeliveriesHonoursLimit and
// DropDeliveriesOnlyAffectsThatAggregate check the delivery log's ordering,
// limit and scoping. AckOfUnknownDeliveryIsNotAnError and
// AckAfterDropIsNotAnError check that acknowledging is idempotent.
// DropThenEnqueueInOneTxKeepsTheNewEntry checks that a Forget can drop an
// aggregate's entries and record its tombstone in one transaction, the new
// entry surviving the drop and sorting after the others.
// DeliveryKeyCanBeEnqueuedAgain checks that an acknowledged or dropped key may
// be enqueued again, since a forgotten aggregate restarts at version 1.
//
// Concurrency. ConcurrentAppendsOfOneVersionAdmitOne checks that of many
// writers racing for one version exactly one commits and the rest fail with
// models.ErrPipelineFailed. ConcurrentAppendsOfDistinctVersionsAllCommit
// checks that writers that do not conflict all commit and nothing is lost.
func Run(
	t *testing.T,
	newStore Factory,
) {
	t.Helper()
	runScenarios(
		t,
		storeScenarios(),
		newStore,
	)
}

func storeScenarios() map[string]scenario[models.Store] {
	scenarios := map[string]scenario[models.Store]{}
	for _, group := range []map[string]scenario[models.Store]{
		appendScenarios(),
		transactionScenarios(),
		deliveryScenarios(),
	} {
		maps.Copy(
			scenarios,
			group,
		)
	}

	return scenarios
}

func deliveryOf(
	aggregateID string,
	version int64,
) models.Delivery {
	return models.Delivery{AggregateID: aggregateID, Version: version}
}

func successor(
	d models.Delivery,
) models.Delivery {
	return models.Delivery{AggregateID: d.AggregateID, Version: d.Version + 1}
}
