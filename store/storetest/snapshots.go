package storetest

import (
	"bytes"
	"context"
	"github.com/char2cs/asynx/models"
	"testing"
)

const (
	roundTripVersion = 7
	oldVersion       = 5
	newerVersion     = 2
	snapshotWriters  = 16
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
			snapshotCase(
				fn,
				newStore,
			),
		)
	}
}

func snapshotCase(
	fn scenario[models.SnapshotStore],
	newStore SnapshotFactory,
) func(*testing.T) {
	return func(
		t *testing.T,
	) {
		fn(
			t,
			newStore(t),
		)
	}
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

func mustPut(
	t testing.TB,
	s models.SnapshotStore,
	aggregateID string,
	version int64,
	data string,
) {
	t.Helper()
	err := s.Put(
		context.Background(),
		aggregateID,
		version,
		[]byte(data),
	)
	if err != nil {
		t.Fatalf(
			"Put %s v%d: %v",
			aggregateID,
			version,
			err,
		)
	}
}

func mustDelete(
	t testing.TB,
	s models.SnapshotStore,
	aggregateID string,
) {
	t.Helper()
	err := s.Delete(
		context.Background(),
		aggregateID,
	)
	if err != nil {
		t.Fatalf(
			"Delete %s: %v",
			aggregateID,
			err,
		)
	}
}

func mustGet(
	t testing.TB,
	s models.SnapshotStore,
	aggregateID string,
) (data []byte, found bool) {
	t.Helper()
	data, found, err := s.Get(
		context.Background(),
		aggregateID,
	)
	if err != nil {
		t.Fatalf(
			"Get %s: %v",
			aggregateID,
			err,
		)
	}

	return data,
		found
}

func snapshotRoundTrip(
	t testing.TB,
	s models.SnapshotStore,
) {
	ctx := context.Background()
	want := []byte{0, 1, 2, 255, '{', '}'}
	err := s.Put(
		ctx,
		"a",
		roundTripVersion,
		want,
	)
	if err != nil {
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
	for v, data := range []string{"one", "two", "three"} {
		mustPut(
			t,
			s,
			"a",
			int64(v+1),
			data,
		)
	}

	got, found, err := s.Get(
		context.Background(),
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
	mustPut(
		t,
		s,
		"a",
		1,
		"A",
	)
	mustPut(
		t,
		s,
		"b",
		1,
		"B",
	)
	mustDelete(
		t,
		s,
		"a",
	)

	got, found := mustGet(
		t,
		s,
		"b",
	)
	if !found ||
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
	mustPut(
		t,
		s,
		"a",
		1,
		"A",
	)
	mustDelete(
		t,
		s,
		"a",
	)

	_, found := mustGet(
		t,
		s,
		"a",
	)
	if found {
		t.Fatal("snapshot survived Delete")
	}

	mustDelete(
		t,
		s,
		"a",
	)
	mustDelete(
		t,
		s,
		"never-existed",
	)
}

func snapshotPutAfterDelete(
	t testing.TB,
	s models.SnapshotStore,
) {
	mustPut(
		t,
		s,
		"a",
		oldVersion,
		"old",
	)
	mustDelete(
		t,
		s,
		"a",
	)
	mustPut(
		t,
		s,
		"a",
		1,
		"fresh",
	)

	got, found := mustGet(
		t,
		s,
		"a",
	)
	if !found ||
		string(got) != "fresh" {
		t.Fatalf(
			"Get = %q, %v; want the snapshot written after the delete",
			got,
			found,
		)
	}
}

func writerPayload(
	i int,
) []byte {
	return []byte{'v', byte('a' + i)}
}

func putAsWriter(
	ctx context.Context,
	s models.SnapshotStore,
) func(int) error {
	return func(
		i int,
	) error {
		return s.Put(
			ctx,
			"a",
			int64(i+1),
			writerPayload(i),
		)
	}
}

func writerCandidates() map[string]struct{} {
	candidates := map[string]struct{}{}
	for i := range snapshotWriters {
		candidates[string(writerPayload(i))] = struct{}{}
	}

	return candidates
}

func reportPutErrors(
	t testing.TB,
	results []error,
) {
	t.Helper()
	for i, err := range results {
		reportPutError(
			t,
			i,
			err,
		)
	}
}

func reportPutError(
	t testing.TB,
	i int,
	err error,
) {
	t.Helper()
	if err == nil {
		return
	}

	t.Errorf(
		"Put v%d: %v",
		i+1,
		err,
	)
}

func snapshotConcurrentPuts(
	t testing.TB,
	s models.SnapshotStore,
) {
	ctx := context.Background()
	results := raceAll(
		snapshotWriters,
		putAsWriter(
			ctx,
			s,
		),
	)
	reportPutErrors(
		t,
		results,
	)

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

	if _, ok := writerCandidates()[string(got)]; !ok {
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
	mustPut(
		t,
		s,
		"a",
		1,
		"A",
	)
	done, cancel := context.WithCancel(context.Background())
	cancel()
	failsOnCancelled(
		done,
		t,
		s,
	)

	got, found := mustGet(
		t,
		s,
		"a",
	)
	if !found ||
		string(got) != "A" {
		t.Errorf(
			"the snapshot changed after failed operations: %q, %v",
			got,
			found,
		)
	}
}

func failsOnCancelled(
	done context.Context,
	t testing.TB,
	s models.SnapshotStore,
) {
	t.Helper()
	if err := s.Put(
		done,
		"a",
		newerVersion,
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
