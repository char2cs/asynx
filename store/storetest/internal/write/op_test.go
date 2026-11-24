package write_test

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/storage"
	"github.com/char2cs/asynx/store"
	"github.com/char2cs/asynx/store/storetest/internal/write"
	"testing"
)

var errAbort = errors.New("abort")

func readStream(
	t *testing.T,
	s storage.Store,
	streamID string,
) [][]byte {
	t.Helper()
	blobs, err := s.ReadFrom(
		context.Background(),
		streamID,
		1,
	)
	if err != nil {
		t.Fatalf(
			"ReadFrom(%s): %v",
			streamID,
			err,
		)
	}

	return blobs
}

func pendingOf(
	t *testing.T,
	s storage.Store,
	limit int,
) []event.Delivery {
	t.Helper()
	pending, err := s.PendingDeliveries(
		context.Background(),
		limit,
	)
	if err != nil {
		t.Fatalf(
			"PendingDeliveries: %v",
			err,
		)
	}

	return pending
}

func TestAbort_FailsWithItsErrorAndWritesNothing(
	t *testing.T,
) {
	s := store.New()

	err := s.Atomically(
		context.Background(),
		write.Transaction(
			write.Abort(errAbort),
			write.Blob(
				"s1",
				1,
				[]byte("a"),
			),
		),
	)
	if !errors.Is(
		err,
		errAbort,
	) {
		t.Fatalf(
			"Atomically = %v, want the abort error",
			err,
		)
	}

	blobs := readStream(
		t,
		s,
		"s1",
	)
	if len(blobs) != 0 {
		t.Fatalf(
			"ops after an abort ran: %q",
			blobs,
		)
	}
}

func TestNative_RunsInsideATransactionWithoutFailingIt(
	t *testing.T,
) {
	err := store.New().Atomically(
		context.Background(),
		write.Transaction(write.Native()),
	)
	if err != nil {
		t.Fatalf(
			"Atomically = %v, want nil",
			err,
		)
	}
}
