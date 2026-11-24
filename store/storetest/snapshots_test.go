package storetest

import (
	"github.com/char2cs/asynx/v2/models/storage"
	"github.com/char2cs/asynx/v2/store"
	"testing"
)

func TestMemorySnapshotStoreConforms(
	t *testing.T,
) {
	RunSnapshots(
		t,
		memorySnapshots,
	)
}

func memorySnapshots(
	*testing.T,
) storage.SnapshotStore {
	return store.NewSnapshots()
}

func TestSnapshotSuite_FlagsStoresThatDoNotKeepWhatTheyAccepted(
	t *testing.T,
) {
	failed := failures(
		snapshotScenarios(),
		storage.SnapshotStore(forgetfulSnapshots{SnapshotStore: store.NewSnapshots()}),
	)
	if !failed["PutThenGetReturnsTheBytes"] ||
		!failed["PutReplacesTheSnapshot"] {
		t.Fatalf(
			"a snapshot store that keeps nothing passed: %v",
			failed,
		)
	}
}

func TestSnapshotSuite_FlagsStoresThatFail(
	t *testing.T,
) {
	failed := failures(
		snapshotScenarios(),
		storage.SnapshotStore(failingSnapshots{SnapshotStore: store.NewSnapshots()}),
	)

	if got, total := countFailed(failed), len(failed); got < total/2 {
		t.Fatalf(
			"only %d of %d scenarios failed a store that errors on everything",
			got,
			total,
		)
	}
}
