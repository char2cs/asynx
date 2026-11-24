package storetest

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/models"
	"testing"
	"time"
)

func mustAtomically(
	t testing.TB,
	s models.Store,
	fn func(models.Tx) error,
) {
	t.Helper()
	if err := s.Atomically(
		context.Background(),
		fn,
	); err != nil {
		t.Fatalf(
			"Atomically: %v",
			err,
		)
	}
}

func duplicateVersionRollsBack(
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	mustAtomically(
		t,
		s,
		func(
			tx models.Tx,
		) error {
			return tx.Append(
				ctx,
				"s1",
				1,
				[]byte("first"),
			)
		},
	)

	err := s.Atomically(
		ctx,
		func(
			tx models.Tx,
		) error {
			if err := tx.EnqueueDelivery(
				ctx,
				"s1",
				99,
			); err != nil {
				return err
			}

			return tx.Append(
				ctx,
				"s1",
				1,
				[]byte("second"),
			)
		},
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

	blobs, _ := s.ReadFrom(
		ctx,
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

	pending, _ := s.PendingDeliveries(
		ctx,
		10,
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
	ctx := context.Background()
	err := s.Atomically(
		ctx,
		func(
			tx models.Tx,
		) error {
			_ = tx.Append(
				ctx,
				"s1",
				1,
				[]byte("x"),
			)
			_ = tx.PutReceipt(
				ctx,
				"cmd-1",
				models.Receipt{Accepted: true, EventID: "e1", Version: 1},
			)
			_ = tx.EnqueueDelivery(
				ctx,
				"s1",
				1,
			)
			return errBoom
		},
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

	if blobs, _ := s.ReadFrom(
		ctx,
		"s1",
		1,
	); len(blobs) != 0 {
		t.Errorf(
			"append survived rollback: %q",
			blobs,
		)
	}

	if _, found, _ := s.GetReceipt(
		ctx,
		"cmd-1",
	); found {
		t.Error("receipt survived rollback")
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 0 {
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
	enqueueIn(
		t,
		s,
		func(
			ctx context.Context,
			tx models.Tx,
		) error {
			return tx.EnqueueDelivery(
				ctx,
				"a",
				1,
			)
		},
	)
	err := s.Atomically(
		context.Background(),
		func(
			tx models.Tx,
		) error {
			return errors.Join(
				tx.DropDeliveries(
					context.Background(),
					"a",
				),
				errBoom,
			)
		},
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
		func(
			tx models.Tx,
		) error {
			_ = tx.Native()
			return nil
		},
	)
}

func cancelledContext(
	t testing.TB,
	s models.Store,
) {
	live := context.Background()
	mustAtomically(
		t,
		s,
		func(
			tx models.Tx,
		) error {
			return errors.Join(
				tx.Append(
					live,
					"s1",
					1,
					[]byte("a"),
				),
				tx.EnqueueDelivery(
					live,
					"s1",
					1,
				),
			)
		},
	)

	done, cancel := context.WithCancel(live)
	cancel()

	if err := s.Atomically(
		done,
		func(
			tx models.Tx,
		) error {
			return tx.Append(
				live,
				"s1",
				2,
				[]byte("b"),
			)
		},
	); err == nil {
		t.Error("Atomically succeeded on a cancelled context")
	}

	if _, err := s.ReadFrom(
		done,
		"s1",
		1,
	); err == nil {
		t.Error("ReadFrom succeeded on a cancelled context")
	}

	if _, err := s.ReadRange(
		done,
		"s1",
		1,
		1,
	); err == nil {
		t.Error("ReadRange succeeded on a cancelled context")
	}

	if _, _, err := s.GetReceipt(
		done,
		"cmd",
	); err == nil {
		t.Error("GetReceipt succeeded on a cancelled context")
	}

	if _, err := s.PendingDeliveries(
		done,
		10,
	); err == nil {
		t.Error("PendingDeliveries succeeded on a cancelled context")
	}

	if err := s.AckDelivery(
		done,
		"s1",
		1,
	); err == nil {
		t.Error("AckDelivery succeeded on a cancelled context")
	}

	if blobs, _ := s.ReadFrom(
		live,
		"s1",
		1,
	); len(blobs) != 1 {
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

func readsDuringOpenTransaction(
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	appended := make(chan struct{})
	release := make(chan struct{})
	committed := make(
		chan error,
		1,
	)
	go func() {
		committed <- s.Atomically(
			ctx,
			func(
				tx models.Tx,
			) error {
				return appendAndHold(
					ctx,
					tx,
					appended,
					release,
				)
			},
		)
	}()
	select {
	case <-appended:
	case err := <-committed:
		t.Fatalf(
			"the transaction ended (%v) without running its function",
			err,
		)
	}

	read := make(
		chan error,
		1,
	)
	go func() {
		_, err := s.ReadFrom(
			ctx,
			"s1",
			1,
		)
		read <- err
	}()
	var readErr error
	select {
	case readErr = <-read:
		read = nil
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	if err := <-committed; err != nil {
		t.Fatalf(
			"the open transaction failed to commit: %v",
			err,
		)
	}

	if read != nil {
		readErr = <-read
	}

	if readErr != nil {
		t.Errorf(
			"a read during an open transaction failed: %v",
			readErr,
		)
	}

	if blobs, err := s.ReadFrom(
		ctx,
		"s1",
		1,
	); err != nil ||
		len(blobs) != 1 {
		t.Fatalf(
			"after the commit ReadFrom = %q, %v; want the record",
			blobs,
			err,
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
