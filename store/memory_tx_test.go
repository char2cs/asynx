package store

import (
	"context"
	"errors"
	asynxmd "github.com/char2cs/asynx/models"
	"testing"
)

func mustAtomically(
	t *testing.T,
	s Memory,
	fn func(tx asynxmd.Tx) error,
) {
	t.Helper()

	err := s.Atomically(
		context.Background(),
		fn,
	)
	if err != nil {
		t.Fatalf(
			"atomically: %v",
			err,
		)
	}
}

func mustAppend(
	t *testing.T,
	s Memory,
	id string,
	data string,
) {
	t.Helper()

	err := s.Append(
		context.Background(),
		id,
		1,
		[]byte(data),
	)
	if err != nil {
		t.Fatalf(
			"append: %v",
			err,
		)
	}
}

func readBlobs(
	t *testing.T,
	s Memory,
	id string,
) [][]byte {
	t.Helper()

	blobs, err := s.ReadFrom(
		context.Background(),
		id,
		1,
	)
	if err != nil {
		t.Fatalf(
			"read: %v",
			err,
		)
	}

	return blobs
}

func mustAppendAt(
	t *testing.T,
	s Memory,
	streamID string,
	version int64,
	data []byte,
) {
	t.Helper()

	err := s.Append(
		context.Background(),
		streamID,
		version,
		data,
	)
	if err != nil {
		t.Fatalf(
			"append %s v%d: %v",
			streamID,
			version,
			err,
		)
	}
}

func readBlobsFrom(
	t *testing.T,
	s Memory,
	streamID string,
	from int64,
) [][]byte {
	t.Helper()

	blobs, err := s.ReadFrom(
		context.Background(),
		streamID,
		from,
	)
	if err != nil {
		t.Fatalf(
			"read %s from %d: %v",
			streamID,
			from,
			err,
		)
	}

	return blobs
}

func readBlobsInRange(
	t *testing.T,
	s Memory,
	streamID string,
	from int64,
	count int64,
) [][]byte {
	t.Helper()

	blobs, err := s.ReadRange(
		context.Background(),
		streamID,
		from,
		count,
	)
	if err != nil {
		t.Fatalf(
			"read range %s from %d: %v",
			streamID,
			from,
			err,
		)
	}

	return blobs
}

func pendingCount(
	t *testing.T,
	s Memory,
) []asynxmd.Delivery {
	t.Helper()

	pending, err := s.PendingDeliveries(
		context.Background(),
		10,
	)
	if err != nil {
		t.Fatalf(
			"pending: %v",
			err,
		)
	}

	return pending
}

func checkNativeNil(
	t *testing.T,
	tx asynxmd.Tx,
) error {
	if tx.Native() != nil {
		t.Error("Native() should be nil for the in-memory store")
	}

	return nil
}

func TestAtomically_NativeIsNilForMemory(
	t *testing.T,
) {
	check := func(
		tx asynxmd.Tx,
	) error {
		return checkNativeNil(
			t,
			tx,
		)
	}

	mustAtomically(
		t,
		New(),
		check,
	)
}

func TestAtomically_CancelledContextDoesNotRun(
	t *testing.T,
) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ran := false
	fn := func(
		asynxmd.Tx,
	) error {
		ran = true
		return nil
	}

	err := New().Atomically(
		ctx,
		fn,
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

func cancelledChecks(
	dead context.Context,
	tx asynxmd.Tx,
) map[string]error {
	return map[string]error{
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
}

func expectAllCanceled(
	t *testing.T,
	checks map[string]error,
) error {
	for name, err := range checks {
		expectCanceled(
			t,
			name,
			err,
		)
	}

	return nil
}

func expectCanceled(
	t *testing.T,
	name string,
	err error,
) {
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

func TestAtomically_TxMethodsRejectCancelledContext(
	t *testing.T,
) {
	dead, cancel := context.WithCancel(context.Background())
	cancel()
	check := func(
		tx asynxmd.Tx,
	) error {
		return expectAllCanceled(
			t,
			cancelledChecks(
				dead,
				tx,
			),
		)
	}

	mustAtomically(
		t,
		New(),
		check,
	)
}

func TestAtomically_RollbackRestoresDeletedStreamAndDroppedDeliveries(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()
	mustAppend(
		t,
		s,
		"s1",
		"keep",
	)
	enqueue := func(
		tx asynxmd.Tx,
	) error {
		return tx.EnqueueDelivery(
			ctx,
			"s1",
			1,
		)
	}
	mustAtomically(
		t,
		s,
		enqueue,
	)

	boom := errors.New("boom")
	destroy := func(
		tx asynxmd.Tx,
	) error {
		return errors.Join(
			tx.DeleteStream(
				ctx,
				"s1",
			),
			tx.DropDeliveries(
				ctx,
				"s1",
			),
			boom,
		)
	}

	err := s.Atomically(
		ctx,
		destroy,
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

	blobs := readBlobs(
		t,
		s,
		"s1",
	)
	if len(blobs) != 1 ||
		string(blobs[0]) != "keep" {
		t.Errorf(
			"stream after rollback = %q, want [keep]",
			blobs,
		)
	}

	pending := pendingCount(
		t,
		s,
	)
	if len(pending) != 1 {
		t.Errorf(
			"pending after rollback = %v, want 1 entry",
			pending,
		)
	}
}

func requirePanic(
	t *testing.T,
) {
	if recover() == nil {
		t.Error("panic was swallowed")
	}
}

func reportErr(
	t *testing.T,
	err error,
) {
	if err != nil {
		t.Errorf(
			"append: %v",
			err,
		)
	}
}

func atomicallyPanics(
	t *testing.T,
	s Memory,
) {
	ctx := context.Background()
	fn := func(
		tx asynxmd.Tx,
	) error {
		reportErr(
			t,
			tx.Append(
				ctx,
				"s1",
				1,
				[]byte("x"),
			),
		)
		panic("fn blew up")
	}
	defer requirePanic(t)

	err := s.Atomically(
		ctx,
		fn,
	)
	t.Errorf(
		"atomically returned %v instead of panicking",
		err,
	)
}

func TestAtomically_PanicRollsBackAndRepanics(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()

	atomicallyPanics(
		t,
		s,
	)

	blobs := readBlobs(
		t,
		s,
		"s1",
	)
	if len(blobs) != 0 {
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
	fn := func(
		tx asynxmd.Tx,
	) error {
		return errors.Join(
			tx.EnqueueDelivery(
				ctx,
				"s1",
				1,
			),
			tx.Append(
				ctx,
				"s1",
				1,
				nil,
			),
		)
	}

	err := s.Atomically(
		ctx,
		fn,
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

	pending := pendingCount(
		t,
		s,
	)
	if len(pending) != 0 {
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
	fn := func(
		tx asynxmd.Tx,
	) error {
		return tx.PutReceipt(
			ctx,
			"c1",
			asynxmd.Receipt{Accepted: true},
		)
	}
	put := func() error {
		return s.Atomically(
			ctx,
			fn,
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

func TestAtomically_RollbackRemovesOnlyTheVersionsItInserted(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()
	mustAppendAt(
		t,
		s,
		"s1",
		2,
		[]byte("two"),
	)
	mustAppendAt(
		t,
		s,
		"s1",
		4,
		[]byte("four"),
	)
	boom := errors.New("boom")
	interleave := func(
		tx asynxmd.Tx,
	) error {
		return errors.Join(
			tx.Append(
				ctx,
				"s1",
				3,
				[]byte("three"),
			),
			tx.Append(
				ctx,
				"s1",
				1,
				[]byte("one"),
			),
			tx.Append(
				ctx,
				"s1",
				5,
				[]byte("five"),
			),
			boom,
		)
	}

	err := s.Atomically(
		ctx,
		interleave,
	)
	if !errors.Is(
		err,
		boom,
	) {
		t.Fatalf(
			"err = %v, want boom",
			err,
		)
	}

	blobs := readBlobsFrom(
		t,
		s,
		"s1",
		1,
	)
	if len(blobs) != 2 ||
		string(blobs[0]) != "two" ||
		string(blobs[1]) != "four" {
		t.Fatalf(
			"stream after rollback = %q, want [two four]",
			blobs,
		)
	}
}
