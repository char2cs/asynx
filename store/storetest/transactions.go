package storetest

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/v2/models/event"
	"github.com/char2cs/asynx/v2/models/failure"
	"github.com/char2cs/asynx/v2/models/storage"
	"github.com/char2cs/asynx/v2/store/storetest/internal/held"
	"github.com/char2cs/asynx/v2/store/storetest/internal/write"
	"testing"
	"time"
)

const readWait = 100 * time.Millisecond

func duplicateVersionRollsBack(
	t testing.TB,
	s storage.Store,
) {
	const unrelatedVersion = 99
	f := newFixture(
		t,
		s,
	)
	f.commit(write.Blobs(
		"s1",
		"first",
	)...)

	err := f.try(
		write.Delivery(deliveryOf(
			"s1",
			unrelatedVersion,
		)),
		write.Blob(
			"s1",
			1,
			[]byte("second"),
		),
	)
	if !errors.Is(
		err,
		failure.ErrPipelineFailed,
	) {
		t.Fatalf(
			"duplicate Append err = %v, want ErrPipelineFailed",
			err,
		)
	}

	f.wantBlobsFrom(
		"s1",
		1,
		"first",
	)
	f.wantPending()
}

func errorRollsBackEverything(
	t testing.TB,
	s storage.Store,
) {
	f := newFixture(
		t,
		s,
	)
	err := f.try(
		write.Blob(
			"s1",
			1,
			[]byte("x"),
		),
		write.Receipt(
			"cmd-1",
			event.Receipt{Accepted: true, EventID: "e1", Version: 1},
		),
		write.Delivery(deliveryOf(
			"s1",
			1,
		)),
		write.Abort(errBoom),
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

	f.wantBlobsFrom(
		"s1",
		1,
	)
	f.wantNoReceipt("cmd-1")
	f.wantPending()
}

func rollbackRestoresDrop(
	t testing.TB,
	s storage.Store,
) {
	a1 := deliveryOf(
		"a",
		1,
	)
	f := newFixture(
		t,
		s,
	)
	f.commit(write.Delivery(a1))

	err := f.try(
		write.EraseDeliveries("a"),
		write.Abort(errBoom),
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

	f.wantPending(a1)
}

func nativeCallable(
	t testing.TB,
	s storage.Store,
) {
	newFixture(
		t,
		s,
	).commit(write.Native())
}

func cancelledContext(
	t testing.TB,
	s storage.Store,
) {
	f := newFixture(
		t,
		s,
	)
	a1 := deliveryOf(
		"s1",
		1,
	)
	f.commit(
		write.Blob(
			"s1",
			1,
			[]byte("a"),
		),
		write.Delivery(a1),
	)

	done, cancel := context.WithCancel(context.Background())
	cancel()

	wantAllFailed(
		t,
		f.cancelledOutcomes(done),
	)
	f.wantBlobsFrom(
		"s1",
		1,
		"a",
	)
	f.wantPending(a1)
}

func readsDuringOpenTransaction(
	t testing.TB,
	s storage.Store,
) {
	f := newFixture(
		t,
		s,
	)
	open := held.Begin(
		t,
		s,
	)
	read := make(
		chan error,
		1,
	)
	go func() {
		read <- f.tryReadFrom(context.Background())
	}()
	time.Sleep(readWait)
	open.Commit(t)

	if err := <-read; err != nil {
		t.Errorf(
			"a read during an open transaction failed: %v",
			err,
		)
	}

	f.wantBlobsFrom(
		"s1",
		1,
		"a",
	)
}

func transactionScenarios() map[string]scenario[storage.Store] {
	return map[string]scenario[storage.Store]{
		"DuplicateVersionConflictsAndRollsBack": duplicateVersionRollsBack,
		"ErrorRollsBackEverything":              errorRollsBackEverything,
		"RollbackRestoresDroppedDeliveries":     rollbackRestoresDrop,
		"NativeIsCallable":                      nativeCallable,
		"CancelledContextFailsEveryOperation":   cancelledContext,
		"ReadsDuringAnOpenTransactionDoNotFail": readsDuringOpenTransaction,
	}
}
