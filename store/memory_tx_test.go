package store

import (
	"context"
	"errors"
	asynxmd "github.com/char2cs/asynx/models"
	"testing"
)

func TestAtomically_NativeIsNilForMemory(
	t *testing.T,
) {
	s := New()
	err := s.Atomically(
		context.Background(),
		func(
			tx asynxmd.Tx,
		) error {
			if tx.Native() != nil {
				t.Error("Native() should be nil for the in-memory store")
			}

			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestAtomically_CancelledContextDoesNotRun(
	t *testing.T,
) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ran := false
	err := New().Atomically(
		ctx,
		func(
			asynxmd.Tx,
		) error {
			ran = true
			return nil
		},
	)
	if !errors.Is(
		err,
		context.Canceled,
	) ||
		ran {
		t.Fatalf(
			"err = %v, ran = %v; want context.Canceled and no run",
			err,
			ran,
		)
	}
}

func TestAtomically_TxMethodsRejectCancelledContext(
	t *testing.T,
) {
	s := New()
	live := context.Background()
	dead, cancel := context.WithCancel(live)
	cancel()

	s.Atomically(
		live,
		func(
			tx asynxmd.Tx,
		) error {
			checks := map[string]error{
				"Append": tx.Append(
					dead,
					"a",
					1,
					nil,
				),
				"DeleteStream": tx.DeleteStream(
					dead,
					"a",
				),
				"DropDeliveries": tx.DropDeliveries(
					dead,
					"a",
				),
				"PutReceipt": tx.PutReceipt(
					dead,
					"c",
					asynxmd.Receipt{},
				),
				"EnqueueDelivery": tx.EnqueueDelivery(
					dead,
					"a",
					1,
				),
			}
			for name, err := range checks {
				if !errors.Is(
					err,
					context.Canceled,
				) {
					t.Errorf(
						"%s err = %v, want context.Canceled",
						name,
						err,
					)
				}
			}

			return nil
		},
	)
}

func TestAtomically_RollbackRestoresDeletedStreamAndDroppedDeliveries(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()
	s.Append(
		ctx,
		"s1",
		1,
		[]byte("keep"),
	)
	s.Atomically(
		ctx,
		func(
			tx asynxmd.Tx,
		) error {
			return tx.EnqueueDelivery(
				ctx,
				"s1",
				1,
			)
		},
	)

	boom := errors.New("boom")
	err := s.Atomically(
		ctx,
		func(
			tx asynxmd.Tx,
		) error {
			tx.DeleteStream(
				ctx,
				"s1",
			)
			tx.DropDeliveries(
				ctx,
				"s1",
			)
			return boom
		},
	)
	if !errors.Is(
		err,
		boom,
	) {
		t.Fatalf(
			"err = %v",
			err,
		)
	}

	if blobs, _ := s.ReadFrom(
		ctx,
		"s1",
		1,
	); len(blobs) != 1 ||
		string(blobs[0]) != "keep" {
		t.Errorf(
			"stream after rollback = %q, want [keep]",
			blobs,
		)
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 1 {
		t.Errorf(
			"pending after rollback = %v, want 1 entry",
			pending,
		)
	}
}

func TestAtomically_PanicRollsBackAndRepanics(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()

	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic was swallowed")
			}
		}()
		s.Atomically(
			ctx,
			func(
				tx asynxmd.Tx,
			) error {
				tx.Append(
					ctx,
					"s1",
					1,
					[]byte("x"),
				)
				panic("fn blew up")
			},
		)
	}()

	if blobs, _ := s.ReadFrom(
		ctx,
		"s1",
		1,
	); len(blobs) != 0 {
		t.Errorf(
			"append survived a panic: %q",
			blobs,
		)
	}

	if err := s.Append(
		ctx,
		"s1",
		1,
		[]byte("ok"),
	); err != nil {
		t.Fatalf(
			"store unusable after panic: %v",
			err,
		)
	}
}

func TestAtomically_SetErrorFailsTheTransactionWithThatError(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()
	sentinel := errors.New("injected")
	s.SetError(
		"s1",
		sentinel,
	)

	err := s.Atomically(
		ctx,
		func(
			tx asynxmd.Tx,
		) error {
			tx.EnqueueDelivery(
				ctx,
				"s1",
				1,
			)
			return tx.Append(
				ctx,
				"s1",
				1,
				nil,
			)
		},
	)
	if !errors.Is(
		err,
		sentinel,
	) {
		t.Fatalf(
			"err = %v, want sentinel",
			err,
		)
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 0 {
		t.Errorf(
			"delivery survived the failed transaction: %v",
			pending,
		)
	}
}

func TestPutReceipt_DuplicateAcrossTransactionsIsRejected(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()
	put := func() error {
		return s.Atomically(
			ctx,
			func(
				tx asynxmd.Tx,
			) error {
				return tx.PutReceipt(
					ctx,
					"c1",
					asynxmd.Receipt{Accepted: true},
				)
			},
		)
	}
	if err := put(); err != nil {
		t.Fatal(err)
	}

	if err := put(); !errors.Is(
		err,
		asynxmd.ErrDuplicateReceipt,
	) {
		t.Fatalf(
			"err = %v, want ErrDuplicateReceipt",
			err,
		)
	}
}
