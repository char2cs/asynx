package write_test

import (
	"context"
	"github.com/char2cs/asynx/store"
	"github.com/char2cs/asynx/store/storetest/internal/write"
	"testing"
)

func TestBlobs_AppendAtVersionsOneUpwardInOrder(
	t *testing.T,
) {
	s := store.New()

	err := s.Atomically(
		context.Background(),
		write.Transaction(write.Blobs(
			"s1",
			"a",
			"b",
			"c",
		)...),
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
	if len(blobs) != 3 ||
		string(blobs[0]) != "a" ||
		string(blobs[2]) != "c" {
		t.Fatalf(
			"stream = %q, want a b c at versions 1 to 3",
			blobs,
		)
	}
}

func TestEraseStream_RemovesOnlyThatStream(
	t *testing.T,
) {
	s := store.New()
	seed := write.Transaction(
		write.Blob(
			"s1",
			1,
			[]byte("a"),
		),
		write.Blob(
			"s2",
			1,
			[]byte("b"),
		),
	)
	if err := s.Atomically(
		context.Background(),
		seed,
	); err != nil {
		t.Fatal(err)
	}

	err := s.Atomically(
		context.Background(),
		write.Transaction(write.EraseStream("s1")),
	)
	if err != nil {
		t.Fatalf(
			"Atomically = %v, want nil",
			err,
		)
	}

	gone := readStream(
		t,
		s,
		"s1",
	)
	kept := readStream(
		t,
		s,
		"s2",
	)
	if len(gone) != 0 ||
		len(kept) != 1 {
		t.Fatalf(
			"after EraseStream(s1): s1 = %q, s2 = %q",
			gone,
			kept,
		)
	}
}
