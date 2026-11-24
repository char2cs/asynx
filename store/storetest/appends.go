package storetest

import (
	"context"
	"github.com/char2cs/asynx/models"
	"slices"
	"testing"
)

const (
	secondVersion  = 2
	unknownLimit   = 5
	rangeCount     = 2
	wantTwoBlobs   = 2
	singleBlobWant = 1
)

func appendEach(
	ctx context.Context,
	tx models.Tx,
	streamID string,
	blobs ...[]byte,
) (err error) {
	for i := 0; i < len(blobs) && err == nil; i++ {
		err = tx.Append(
			ctx,
			streamID,
			int64(i+1),
			blobs[i],
		)
	}

	return err
}

func appendEachOp(
	ctx context.Context,
	streamID string,
	blobs ...[]byte,
) func(models.Tx) error {
	return func(
		tx models.Tx,
	) error {
		return appendEach(
			ctx,
			tx,
			streamID,
			blobs...,
		)
	}
}

func appendOp(
	ctx context.Context,
	streamID string,
	version int64,
	data []byte,
) func(models.Tx) error {
	return func(
		tx models.Tx,
	) error {
		return tx.Append(
			ctx,
			streamID,
			version,
			data,
		)
	}
}

func appendCommitsAndReads(
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	mustAtomically(
		t,
		s,
		appendEachOp(
			ctx,
			"s1",
			[]byte("a"),
			[]byte("b"),
			[]byte("c"),
		),
	)

	blobs, err := s.ReadFrom(
		ctx,
		"s1",
		secondVersion,
	)
	if err != nil {
		t.Fatalf(
			"ReadFrom: %v",
			err,
		)
	}

	if !slices.Equal(
		asStrings(blobs),
		[]string{"b", "c"},
	) {
		t.Fatalf(
			"ReadFrom(2) = %q, want [b c]",
			blobs,
		)
	}
}

func readRangeLimits(
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	mustAtomically(
		t,
		s,
		appendEachOp(
			ctx,
			"s1",
			[]byte("1"),
			[]byte("2"),
			[]byte("3"),
			[]byte("4"),
		),
	)

	blobs, err := s.ReadRange(
		ctx,
		"s1",
		secondVersion,
		rangeCount,
	)
	if err != nil {
		t.Fatalf(
			"ReadRange: %v",
			err,
		)
	}

	if !slices.Equal(
		asStrings(blobs),
		[]string{"2", "3"},
	) {
		t.Fatalf(
			"ReadRange(2,2) = %q, want [2 3]",
			blobs,
		)
	}
}

func seedTwoStreams(
	ctx context.Context,
	tx models.Tx,
) error {
	err := tx.Append(
		ctx,
		"s1",
		1,
		[]byte("a"),
	)
	if err != nil {
		return err
	}

	return tx.Append(
		ctx,
		"s2",
		1,
		[]byte("b"),
	)
}

func deleteS1AndUnknown(
	ctx context.Context,
	tx models.Tx,
) error {
	err := tx.DeleteStream(
		ctx,
		"s1",
	)
	if err != nil {
		return err
	}

	return tx.DeleteStream(
		ctx,
		"never-existed",
	)
}

func readStream(
	t testing.TB,
	s models.Store,
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

func deleteStream(
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	seed := func(
		tx models.Tx,
	) error {
		return seedTwoStreams(
			ctx,
			tx,
		)
	}
	remove := func(
		tx models.Tx,
	) error {
		return deleteS1AndUnknown(
			ctx,
			tx,
		)
	}
	mustAtomically(
		t,
		s,
		seed,
	)
	mustAtomically(
		t,
		s,
		remove,
	)
	checkDeletionScoped(
		t,
		s,
	)
	appendAgain(
		t,
		s,
	)
}

func appendAgain(
	t testing.TB,
	s models.Store,
) {
	t.Helper()
	mustAtomically(
		t,
		s,
		appendOp(
			context.Background(),
			"s1",
			1,
			[]byte("again"),
		),
	)
}

func checkDeletionScoped(
	t testing.TB,
	s models.Store,
) {
	t.Helper()
	if blobs := readStream(
		t,
		s,
		"s1",
	); len(blobs) != 0 {
		t.Errorf(
			"s1 not deleted: %q",
			blobs,
		)
	}

	if blobs := readStream(
		t,
		s,
		"s2",
	); len(blobs) != singleBlobWant {
		t.Errorf(
			"s2 was affected by deleting s1: %q",
			blobs,
		)
	}
}

func readsOfUnknown(
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	if blobs, err := s.ReadFrom(
		ctx,
		"nope",
		1,
	); err != nil ||
		len(blobs) != 0 {
		t.Errorf(
			"ReadFrom(unknown) = %q, %v",
			blobs,
			err,
		)
	}

	if blobs, err := s.ReadRange(
		ctx,
		"nope",
		1,
		unknownLimit,
	); err != nil ||
		len(blobs) != 0 {
		t.Errorf(
			"ReadRange(unknown) = %q, %v",
			blobs,
			err,
		)
	}
}

func committedVisible(
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	mustAtomically(
		t,
		s,
		appendOp(
			ctx,
			"s1",
			1,
			[]byte("a"),
		),
	)
	mustAtomically(
		t,
		s,
		appendOp(
			ctx,
			"s1",
			secondVersion,
			[]byte("b"),
		),
	)

	blobs := readStream(
		t,
		s,
		"s1",
	)
	if len(blobs) != wantTwoBlobs {
		t.Fatalf(
			"len = %d, want 2",
			len(blobs),
		)
	}
}

func appendVersionsOutOfOrder(
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	for _, v := range []int64{3, 1, secondVersion} {
		mustAtomically(
			t,
			s,
			appendOp(
				ctx,
				"s1",
				v,
				[]byte{byte('0' + v)},
			),
		)
	}
}

func expectBlobs(
	t testing.TB,
	label string,
	got [][]byte,
	err error,
	want []string,
) {
	t.Helper()
	if err != nil ||
		!slices.Equal(
			asStrings(got),
			want,
		) {
		t.Fatalf(
			"%s = %q, %v; want %v",
			label,
			got,
			err,
			want,
		)
	}
}

func outOfOrderAppends(
	t testing.TB,
	s models.Store,
) {
	appendVersionsOutOfOrder(
		t,
		s,
	)
	readsInVersionOrder(
		t,
		s,
	)
}

func readsInVersionOrder(
	t testing.TB,
	s models.Store,
) {
	t.Helper()
	ctx := context.Background()
	all, err := s.ReadFrom(
		ctx,
		"s1",
		1,
	)
	expectBlobs(
		t,
		"ReadFrom(1)",
		all,
		err,
		[]string{"1", "2", "3"},
	)

	readsTailAndWindow(
		t,
		s,
	)
}

func readsTailAndWindow(
	t testing.TB,
	s models.Store,
) {
	t.Helper()
	ctx := context.Background()
	tail, err := s.ReadFrom(
		ctx,
		"s1",
		secondVersion,
	)
	expectBlobs(
		t,
		"ReadFrom(2)",
		tail,
		err,
		[]string{"2", "3"},
	)

	window, err := s.ReadRange(
		ctx,
		"s1",
		secondVersion,
		1,
	)
	expectBlobs(
		t,
		"ReadRange(2, 1)",
		window,
		err,
		[]string{"2"},
	)
}

func appendScenarios() map[string]scenario[models.Store] {
	return map[string]scenario[models.Store]{
		"AppendCommitsAndReadFromReturnsInOrder":    appendCommitsAndReads,
		"ReadRangeLimitsCount":                      readRangeLimits,
		"DeleteStreamIsIdempotentAndScoped":         deleteStream,
		"ReadsOfUnknownStreamAreEmpty":              readsOfUnknown,
		"CommitInsideAtomicallyIsVisibleAfterwards": committedVisible,
		"AppendsOutOfVersionOrderReadInOrder":       outOfOrderAppends,
	}
}
