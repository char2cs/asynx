package storetest

import (
	"context"
	"github.com/char2cs/asynx/models"
	"slices"
	"testing"
)

func appendEach(
	ctx context.Context,
	tx models.Tx,
	streamID string,
	blobs ...[]byte,
) error {
	for i, blob := range blobs {
		if err := tx.Append(
			ctx,
			streamID,
			int64(i+1),
			blob,
		); err != nil {
			return err
		}
	}

	return nil
}

func appendCommitsAndReads(
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
			return appendEach(
				ctx,
				tx,
				"s1",
				[]byte("a"),
				[]byte("b"),
				[]byte("c"),
			)
		},
	)

	blobs, err := s.ReadFrom(
		ctx,
		"s1",
		2,
	)
	if err != nil {
		t.Fatalf(
			"ReadFrom: %v",
			err,
		)
	}

	if len(blobs) != 2 ||
		string(blobs[0]) != "b" ||
		string(blobs[1]) != "c" {
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
		func(
			tx models.Tx,
		) error {
			return appendEach(
				ctx,
				tx,
				"s1",
				[]byte("1"),
				[]byte("2"),
				[]byte("3"),
				[]byte("4"),
			)
		},
	)

	blobs, err := s.ReadRange(
		ctx,
		"s1",
		2,
		2,
	)
	if err != nil {
		t.Fatalf(
			"ReadRange: %v",
			err,
		)
	}

	if len(blobs) != 2 ||
		string(blobs[0]) != "2" ||
		string(blobs[1]) != "3" {
		t.Fatalf(
			"ReadRange(2,2) = %q, want [2 3]",
			blobs,
		)
	}
}

func deleteStream(
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
			_ = tx.Append(
				ctx,
				"s1",
				1,
				[]byte("a"),
			)
			return tx.Append(
				ctx,
				"s2",
				1,
				[]byte("b"),
			)
		},
	)
	mustAtomically(
		t,
		s,
		func(
			tx models.Tx,
		) error {
			if err := tx.DeleteStream(
				ctx,
				"s1",
			); err != nil {
				return err
			}

			return tx.DeleteStream(
				ctx,
				"never-existed",
			)
		},
	)

	if blobs, _ := s.ReadFrom(
		ctx,
		"s1",
		1,
	); len(blobs) != 0 {
		t.Errorf(
			"s1 not deleted: %q",
			blobs,
		)
	}

	if blobs, _ := s.ReadFrom(
		ctx,
		"s2",
		1,
	); len(blobs) != 1 {
		t.Errorf(
			"s2 was affected by deleting s1: %q",
			blobs,
		)
	}

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
				[]byte("again"),
			)
		},
	)
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
		5,
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
		func(
			tx models.Tx,
		) error {
			return tx.Append(
				ctx,
				"s1",
				1,
				[]byte("a"),
			)
		},
	)
	mustAtomically(
		t,
		s,
		func(
			tx models.Tx,
		) error {
			return tx.Append(
				ctx,
				"s1",
				2,
				[]byte("b"),
			)
		},
	)
	blobs, _ := s.ReadFrom(
		ctx,
		"s1",
		1,
	)
	if len(blobs) != 2 {
		t.Fatalf(
			"len = %d, want 2",
			len(blobs),
		)
	}
}

func outOfOrderAppends(
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	for _, v := range []int64{3, 1, 2} {
		mustAtomically(
			t,
			s,
			func(
				tx models.Tx,
			) error {
				return tx.Append(
					ctx,
					"s1",
					v,
					[]byte{byte('0' + v)},
				)
			},
		)
	}

	all, err := s.ReadFrom(
		ctx,
		"s1",
		1,
	)
	if err != nil ||
		!slices.Equal(
			asStrings(all),
			[]string{"1", "2", "3"},
		) {
		t.Fatalf(
			"ReadFrom(1) = %q, %v; want [1 2 3] in version order",
			all,
			err,
		)
	}

	tail, err := s.ReadFrom(
		ctx,
		"s1",
		2,
	)
	if err != nil ||
		!slices.Equal(
			asStrings(tail),
			[]string{"2", "3"},
		) {
		t.Fatalf(
			"ReadFrom(2) = %q, %v; want [2 3]",
			tail,
			err,
		)
	}

	window, err := s.ReadRange(
		ctx,
		"s1",
		2,
		1,
	)
	if err != nil ||
		!slices.Equal(
			asStrings(window),
			[]string{"2"},
		) {
		t.Fatalf(
			"ReadRange(2, 1) = %q, %v; want [2]",
			window,
			err,
		)
	}
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
