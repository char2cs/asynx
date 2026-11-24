package storetest

import (
	"bytes"
	"context"
	"errors"
	"github.com/char2cs/asynx/models/storage"
	"github.com/char2cs/asynx/store/storetest/internal/race"
	"github.com/char2cs/asynx/store/storetest/internal/write"
	"slices"
	"testing"
)

// RunSnapshots executes the conformance suite for storage.SnapshotStore
// implementations built by newStore. Each scenario runs as a subtest against
// its own empty store.
//
// The scenarios check that a snapshot store behaves as the single mutable cell
// per aggregate that storage.SnapshotStore describes. GetOfUnknownAggregateIsNotFoundNotAnError
// checks that a missing snapshot is reported as not found, which callers
// answer by replaying from version 1, and not as an error.
// PutThenGetReturnsTheBytes checks that arbitrary bytes round-trip unchanged.
// PutReplacesTheSnapshot checks that Put upserts rather than accumulates, so
// Get returns only the newest. AggregatesAreIndependent checks that deleting
// one aggregate's snapshot leaves the others. DeleteRemovesAndIsIdempotent
// checks that Delete removes the snapshot and that deleting twice, or deleting
// an unknown aggregate, is not an error. PutAfterDeleteStores checks that an
// aggregate can be snapshotted again, even from a lower version, after its
// snapshot was deleted. ConcurrentPutsLeaveOneOfThem checks that concurrent
// writers never produce a torn value: Get returns exactly one writer's bytes.
// CancelledContextFailsEveryOperation checks that a context that is already
// done fails Put, Get and Delete and leaves the stored snapshot unchanged.
func RunSnapshots(
	t *testing.T,
	newStore SnapshotFactory,
) {
	t.Helper()
	runScenarios(
		t,
		snapshotScenarios(),
		newStore,
	)
}

func snapshotUnknown(
	t testing.TB,
	s storage.SnapshotStore,
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
	s storage.SnapshotStore,
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
	s storage.SnapshotStore,
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
	s storage.SnapshotStore,
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

const (
	arbitraryVersion = 7
	staleVersion     = 5
)

func snapshotRoundTrip(
	t testing.TB,
	s storage.SnapshotStore,
) {
	ctx := context.Background()
	want := []byte{0, 1, 2, 255, '{', '}'}
	err := s.Put(
		ctx,
		"a",
		arbitraryVersion,
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
	s storage.SnapshotStore,
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
	s storage.SnapshotStore,
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
	s storage.SnapshotStore,
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
	s storage.SnapshotStore,
) {
	mustPut(
		t,
		s,
		"a",
		staleVersion,
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

func snapshotConcurrentPuts(
	t testing.TB,
	s storage.SnapshotStore,
) {
	const writers = 16
	results := race.All(
		writers,
		write.SnapshotPuts(
			s,
			"a",
			writerPayload,
		),
	)

	if err := errors.Join(results...); err != nil {
		t.Errorf(
			"concurrent Put failed: %v",
			err,
		)
	}

	wantOneWritersBytes(
		t,
		s,
		writers,
	)
}

func wantOneWritersBytes(
	t testing.TB,
	s storage.SnapshotStore,
	writers int,
) {
	t.Helper()
	got, found := mustGet(
		t,
		s,
		"a",
	)
	if !found ||
		!slices.ContainsFunc(
			writtenPayloads(writers),
			bytesEqualTo(got),
		) {
		t.Fatalf(
			"Get = %q, %v; want exactly one writer's bytes (no torn write)",
			got,
			found,
		)
	}
}

func writtenPayloads(
	writers int,
) [][]byte {
	payloads := make(
		[][]byte,
		writers,
	)
	for i := range writers {
		payloads[i] = writerPayload(i)
	}

	return payloads
}

func bytesEqualTo(
	want []byte,
) func([]byte) bool {
	return func(
		got []byte,
	) bool {
		return bytes.Equal(
			got,
			want,
		)
	}
}

func snapshotCancelled(
	t testing.TB,
	s storage.SnapshotStore,
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

	wantAllFailed(
		t,
		cancelledSnapshotOutcomes(
			done,
			s,
		),
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

func cancelledSnapshotOutcomes(
	done context.Context,
	s storage.SnapshotStore,
) map[string]error {
	const newerVersion = 2
	outcomes := map[string]error{
		"Put": s.Put(
			done,
			"a",
			newerVersion,
			[]byte("B"),
		),
		"Delete": s.Delete(
			done,
			"a",
		),
	}
	_, _, outcomes["Get"] = s.Get(
		done,
		"a",
	)

	return outcomes
}

func snapshotScenarios() map[string]scenario[storage.SnapshotStore] {
	return map[string]scenario[storage.SnapshotStore]{
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
