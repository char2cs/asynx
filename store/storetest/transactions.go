package storetest

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/models"
	"testing"
	"time"
)

func duplicateVersionRollsBack(
	t testing.TB,
	s models.Store,
) {
	commit(
		t,
		s,
		opAppend(
			"s1",
			1,
			[]byte("first"),
		),
	)
	err := s.Atomically(
		context.Background(),
		runOps(
			opEnqueue(models.Delivery{AggregateID: "s1", Version: conflictVersion}),
			opAppend(
				"s1",
				1,
				[]byte("second"),
			),
		),
	)
	if !errors.Is(
		err,
		models.ErrPipelineFailed,
	) {
		t.Fatalf(
			"duplicate Append err = %v, want ErrPipelineFailed",
			err,
		)
	}

	wantFirstOnly(
		t,
		s,
	)
}

func wantFirstOnly(
	t testing.TB,
	s models.Store,
) {
	blobs := blobsOrFatal(
		t,
		s,
		"s1",
		1,
	)
	if len(blobs) != 1 ||
		string(blobs[0]) != "first" {
		t.Fatalf(
			"stream after conflict = %q, want [first]",
			blobs,
		)
	}

	pending := pendingOrFatal(
		t,
		s,
		listLimit,
	)
	if len(pending) != 0 {
		t.Fatalf(
			"delivery enqueued in a rolled-back tx survived: %v",
			pending,
		)
	}
}

func errorRollsBackEverything(
	t testing.TB,
	s models.Store,
) {
	err := s.Atomically(
		context.Background(),
		runOps(
			opAppend(
				"s1",
				1,
				[]byte("x"),
			),
			opReceipt(
				"cmd-1",
				models.Receipt{Accepted: true, EventID: "e1", Version: 1},
			),
			opEnqueue(models.Delivery{AggregateID: "s1", Version: 1}),
			opFail(errBoom),
		),
	)
	if !errors.Is(
		err,
		errBoom,
	) {
		t.Fatalf(
			"Atomically err = %v, want errBoom",
			err,
		)
	}

	wantNothingSurvived(
		t,
		s,
	)
}

func wantNothingSurvived(
	t testing.TB,
	s models.Store,
) {
	blobs := blobsOrFatal(
		t,
		s,
		"s1",
		1,
	)
	if len(blobs) != 0 {
		t.Errorf(
			"append survived rollback: %q",
			blobs,
		)
	}

	if receiptOrFatal(
		t,
		s,
		"cmd-1",
	) {
		t.Error("receipt survived rollback")
	}

	pending := pendingOrFatal(
		t,
		s,
		listLimit,
	)
	if len(pending) != 0 {
		t.Errorf(
			"delivery survived rollback: %v",
			pending,
		)
	}
}

func rollbackRestoresDrop(
	t testing.TB,
	s models.Store,
) {
	commit(
		t,
		s,
		opEnqueue(models.Delivery{AggregateID: "a", Version: 1}),
	)
	err := s.Atomically(
		context.Background(),
		runOps(
			opDrop("a"),
			opFail(errBoom),
		),
	)
	if !errors.Is(
		err,
		errBoom,
	) {
		t.Fatalf(
			"Atomically = %v, want the callback's error",
			err,
		)
	}

	wantPending(
		t,
		s,
		models.Delivery{AggregateID: "a", Version: 1},
	)
}

func nativeCallable(
	t testing.TB,
	s models.Store,
) {
	mustAtomically(
		t,
		s,
		callNative,
	)
}

func callNative(
	tx models.Tx,
) error {
	_ = tx.Native()
	return nil
}

func cancelledContext(
	t testing.TB,
	s models.Store,
) {
	commit(
		t,
		s,
		opAppend(
			"s1",
			1,
			[]byte("a"),
		),
		opEnqueue(models.Delivery{AggregateID: "s1", Version: 1}),
	)

	done, cancel := context.WithCancel(context.Background())
	cancel()

	wantCancelled(
		t,
		"Atomically",
		s.Atomically(
			done,
			runOps(opAppend(
				"s1",
				versionTwo,
				[]byte("b"),
			)),
		),
	)
	cancelledOps(
		done,
		t,
		s,
	)
	wantOnlyCommitted(
		t,
		s,
	)
}

func wantOnlyCommitted(
	t testing.TB,
	s models.Store,
) {
	blobs := blobsOrFatal(
		t,
		s,
		"s1",
		1,
	)
	if len(blobs) != 1 {
		t.Errorf(
			"stream holds %d records, want only the committed one",
			len(blobs),
		)
	}

	wantPending(
		t,
		s,
		models.Delivery{AggregateID: "s1", Version: 1},
	)
}

func holdingOp(
	appended chan<- struct{},
	release <-chan struct{},
) func(models.Tx) error {
	return func(
		tx models.Tx,
	) error {
		return appendAndHold(
			context.Background(),
			tx,
			appended,
			release,
		)
	}
}

func holdTx(
	s models.Store,
	appended chan<- struct{},
	release <-chan struct{},
	committed chan<- error,
) {
	committed <- s.Atomically(
		context.Background(),
		holdingOp(
			appended,
			release,
		),
	)
}

func readOnce(
	s models.Store,
	out chan<- error,
) {
	_, err := s.ReadFrom(
		context.Background(),
		"s1",
		1,
	)
	out <- err
}

func awaitRead(
	read <-chan error,
) (readErr error, waiting <-chan error) {
	select {
	case readErr = <-read:
		return readErr,
			nil
	case <-time.After(readWait):
		return nil,
			read
	}
}

func openHeldTx(
	t testing.TB,
	s models.Store,
) (release chan struct{}, committed chan error) {
	appended := make(chan struct{})
	release = make(chan struct{})
	committed = make(
		chan error,
		1,
	)
	go holdTx(
		s,
		appended,
		release,
		committed,
	)
	select {
	case <-appended:
	case err := <-committed:
		t.Fatalf(
			"the transaction ended (%v) without running its function",
			err,
		)
	}

	return release,
		committed
}

func readsDuringOpenTransaction(
	t testing.TB,
	s models.Store,
) {
	release, committed := openHeldTx(
		t,
		s,
	)
	read := make(
		chan error,
		1,
	)
	go readOnce(
		s,
		read,
	)
	readErr, waiting := awaitRead(read)
	close(release)
	wantCommitted(
		t,
		committed,
	)
	if waiting != nil {
		readErr = <-waiting
	}

	if readErr != nil {
		t.Errorf(
			"a read during an open transaction failed: %v",
			readErr,
		)
	}

	wantRecordAfterCommit(
		t,
		s,
	)
}

func wantCommitted(
	t testing.TB,
	committed <-chan error,
) {
	if err := <-committed; err != nil {
		t.Fatalf(
			"the open transaction failed to commit: %v",
			err,
		)
	}
}

func wantRecordAfterCommit(
	t testing.TB,
	s models.Store,
) {
	blobs := blobsOrFatal(
		t,
		s,
		"s1",
		1,
	)
	if len(blobs) != 1 {
		t.Fatalf(
			"after the commit ReadFrom = %q; want the record",
			blobs,
		)
	}
}

func transactionScenarios() map[string]scenario[models.Store] {
	return map[string]scenario[models.Store]{
		"DuplicateVersionConflictsAndRollsBack": duplicateVersionRollsBack,
		"ErrorRollsBackEverything":              errorRollsBackEverything,
		"RollbackRestoresDroppedDeliveries":     rollbackRestoresDrop,
		"NativeIsCallable":                      nativeCallable,
		"CancelledContextFailsEveryOperation":   cancelledContext,
		"ReadsDuringAnOpenTransactionDoNotFail": readsDuringOpenTransaction,
	}
}
