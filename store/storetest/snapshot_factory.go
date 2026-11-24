package storetest

import (
	"github.com/char2cs/asynx/models"
	"testing"
)

// SnapshotFactory returns a fresh, empty snapshot store for one subtest.
type SnapshotFactory func(
	t *testing.T,
) models.SnapshotStore
