package write_test

import (
	"context"
	"github.com/char2cs/asynx/store"
	"github.com/char2cs/asynx/store/storetest/internal/write"
	"testing"
)

func TestTransaction_RunsTheOpsInOrderInOneTransaction(
	t *testing.T,
) {
	s := store.New()

	err := s.Atomically(
		context.Background(),
		write.Transaction(
			write.Blob(
				"s1",
				1,
				[]byte("a"),
			),
			write.Blob(
				"s1",
				2,
				[]byte("b"),
			),
		),
	)
	if err != nil {
		t.Fatalf(
			"Atomically = %v, want nil",
			err,
		)
	}

	blobs := readStream(
		t,
		s,
		"s1",
	)
	if len(blobs) != 2 {
		t.Fatalf(
			"stream holds %d records, want both writes",
			len(blobs),
		)
	}
}

func TestTransaction_WithNoOpsCommitsAnEmptyTransaction(
	t *testing.T,
) {
	err := store.New().Atomically(
		context.Background(),
		write.Transaction(),
	)
	if err != nil {
		t.Fatalf(
			"Atomically = %v, want nil",
			err,
		)
	}
}

func TestTransaction_StopsAtTheFirstFailureAndRollsBack(
	t *testing.T,
) {
	s := store.New()

	err := s.Atomically(
		context.Background(),
		write.Transaction(
			write.Blob(
				"s1",
				1,
				[]byte("a"),
			),
			write.Abort(errAbort),
			write.Blob(
				"s1",
				2,
				[]byte("b"),
			),
		),
	)
	if err != errAbort {
		t.Fatalf(
			"Atomically = %v, want the first failure",
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
			"a failed transaction left %q behind",
			blobs,
		)
	}
}
