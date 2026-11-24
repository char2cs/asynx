package write

import (
	"context"
	"github.com/char2cs/asynx/models"
)

// SnapshotPuts returns the lane function for a concurrent run in which writer i
// stores payload(i) as the snapshot of aggregateID at version i+1, under a
// background context, and returns the error of that Put. Each writer gets its
// own version, so a store that serializes the writes keeps exactly one payload.
func SnapshotPuts(
	s models.SnapshotStore,
	aggregateID string,
	payload func(writer int) []byte,
) func(writer int) error {
	return func(
		writer int,
	) error {
		return s.Put(
			context.Background(),
			aggregateID,
			int64(writer+1),
			payload(writer),
		)
	}
}
