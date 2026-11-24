package storetest

import (
	"github.com/char2cs/asynx/v2/models/storage"
	"testing"
)

// SnapshotFactory returns a fresh, empty snapshot store for one subtest.
type SnapshotFactory func(
	t *testing.T,
) storage.SnapshotStore
