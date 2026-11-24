package storetest

import (
	"bytes"
	"context"
	"github.com/char2cs/asynx/models"
	"testing"
)

// RunSnapshots executes the conformance suite for models.SnapshotStore
// implementations built by newStore.
func RunSnapshots(
	t *testing.T,
	newStore SnapshotFactory,
) {
	t.Helper()

	tests := snapshotScenarios()

	for name, fn := range tests {
		t.Run(
			name,
			func(
				t *testing.T,
			) {
				fn(
					t,
					newStore(t),
				)
			},
		)
	}
}

func asStrings(
	blobs [][]byte,
) []string {
	out := make(
		[]string,
		len(blobs),
	)
	for i, b := range blobs {
		out[i] = string(b)
	}

	return out
}

func snapshotUnknown(
	t testing.TB,
	s models.SnapshotStore,
) {
	data, found, err := s.Get(
		context.Background(),
		"ghost",
	)
	isAbsent := !found && len(data) == 0
	if err != nil ||
		!isAbsent {
		t.Fatalf(
			"Get(unknown) = %q, %v, %v; want not found and no error",
			data,
			found,
			err,
		)
	}
}

func snapshotRoundTrip(
	t testing.TB,
	s models.SnapshotStore,
) {
	ctx := context.Background()
	want := []byte{0, 1, 2, 255, '{', '}'}
	if err := s.Put(
		ctx,
		"a",
		7,
		want,
	); err != nil {
		t.Fatalf(
			"Put: %v",
			err,
		)
	}

	got, found, err := s.Get(
		ctx,
		"a",
	)
	isStored := found && bytes.Equal(
		got,
		want,
	)
	if err != nil ||
		!isStored {
		t.Fatalf(
			"Get = %v, %v, %v; want %v",
			got,
			found,
			err,
			want,
		)
	}
}

func snapshotReplaced(
	t testing.TB,
	s models.SnapshotStore,
) {
	ctx := context.Background()
	for v, data := range []string{"one", "two", "three"} {
		if err := s.Put(
			ctx,
			"a",
			int64(v+1),
			[]byte(data),
		); err != nil {
			t.Fatalf(
				"Put v%d: %v",
				v+1,
				err,
			)
		}
	}

	got, found, err := s.Get(
		ctx,
		"a",
	)
	isNewest := found && string(got) == "three"
	if err != nil ||
		!isNewest {
		t.Fatalf(
			"Get = %q, %v, %v; want the newest snapshot only",
			got,
			found,
			err,
		)
	}
}

func snapshotsIndependent(
	t testing.TB,
	s models.SnapshotStore,
) {
	ctx := context.Background()
	s.Put(
		ctx,
		"a",
		1,
		[]byte("A"),
	)
	s.Put(
		ctx,
		"b",
		1,
		[]byte("B"),
	)
	if err := s.Delete(
		ctx,
		"a",
	); err != nil {
		t.Fatal(err)
	}

	if got, found, _ := s.Get(
		ctx,
		"b",
	); !found ||
		string(got) != "B" {
		t.Fatalf(
			"deleting a's snapshot changed b's: %q, %v",
			got,
			found,
		)
	}
}

func snapshotDeleted(
	t testing.TB,
	s models.SnapshotStore,
) {
	ctx := context.Background()
	s.Put(
		ctx,
		"a",
		1,
		[]byte("A"),
	)
	if err := s.Delete(
		ctx,
		"a",
	); err != nil {
		t.Fatalf(
			"Delete: %v",
			err,
		)
	}

	if _, found, _ := s.Get(
		ctx,
		"a",
	); found {
		t.Fatal("snapshot survived Delete")
	}

	if err := s.Delete(
		ctx,
		"a",
	); err != nil {
		t.Fatalf(
			"second Delete = %v, want nil",
			err,
		)
	}

	if err := s.Delete(
		ctx,
		"never-existed",
	); err != nil {
		t.Fatalf(
			"Delete(unknown) = %v, want nil",
			err,
		)
	}
}

func snapshotPutAfterDelete(
	t testing.TB,
	s models.SnapshotStore,
) {
	ctx := context.Background()
	s.Put(
		ctx,
		"a",
		5,
		[]byte("old"),
	)
	s.Delete(
		ctx,
		"a",
	)
	if err := s.Put(
		ctx,
		"a",
		1,
		[]byte("fresh"),
	); err != nil {
		t.Fatalf(
			"Put after Delete: %v",
			err,
		)
	}

	if got, found, _ := s.Get(
		ctx,
		"a",
	); !found ||
		string(got) != "fresh" {
		t.Fatalf(
			"Get = %q, %v; want the snapshot written after the delete",
			got,
			found,
		)
	}
}

func snapshotConcurrentPuts(
	t testing.TB,
	s models.SnapshotStore,
) {
	const writers = 16
	ctx := context.Background()
	payload := func(
		i int,
	) []byte {
		return []byte{'v', byte('a' + i)}
	}
	candidates := map[string]struct{}{}
	for i := range writers {
		candidates[string(payload(i))] = struct{}{}
	}

	results := raceAll(
		writers,
		func(
			i int,
		) error {
			return s.Put(
				ctx,
				"a",
				int64(i+1),
				payload(i),
			)
		},
	)
	for i, err := range results {
		if err != nil {
			t.Errorf(
				"Put v%d: %v",
				i+1,
				err,
			)
		}
	}

	got, found, err := s.Get(
		ctx,
		"a",
	)
	if err != nil ||
		!found {
		t.Fatalf(
			"Get = %q, %v, %v",
			got,
			found,
			err,
		)
	}

	if _, ok := candidates[string(got)]; !ok {
		t.Fatalf(
			"Get = %q, which no writer wrote (torn write)",
			got,
		)
	}
}

func snapshotCancelled(
	t testing.TB,
	s models.SnapshotStore,
) {
	live := context.Background()
	s.Put(
		live,
		"a",
		1,
		[]byte("A"),
	)
	done, cancel := context.WithCancel(live)
	cancel()

	if err := s.Put(
		done,
		"a",
		2,
		[]byte("B"),
	); err == nil {
		t.Error("Put succeeded on a cancelled context")
	}

	if _, _, err := s.Get(
		done,
		"a",
	); err == nil {
		t.Error("Get succeeded on a cancelled context")
	}

	if err := s.Delete(
		done,
		"a",
	); err == nil {
		t.Error("Delete succeeded on a cancelled context")
	}

	if got, found, _ := s.Get(
		live,
		"a",
	); !found ||
		string(got) != "A" {
		t.Errorf(
			"the snapshot changed after failed operations: %q, %v",
			got,
			found,
		)
	}
}

func snapshotScenarios() map[string]scenario[models.SnapshotStore] {
	return map[string]scenario[models.SnapshotStore]{
		"GetOfUnknownAggregateIsNotFoundNotAnError": snapshotUnknown,
		"PutThenGetReturnsTheBytes":                 snapshotRoundTrip,
		"PutReplacesTheSnapshot":                    snapshotReplaced,
		"AggregatesAreIndependent":                  snapshotsIndependent,
		"DeleteRemovesAndIsIdempotent":              snapshotDeleted,
		"PutAfterDeleteStores":                      snapshotPutAfterDelete,
		"ConcurrentPutsLeaveOneOfThem":              snapshotConcurrentPuts,
		"CancelledContextFailsEveryOperation":       snapshotCancelled,
	}
}
