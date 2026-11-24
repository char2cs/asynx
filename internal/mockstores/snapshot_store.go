package mockstores

import (
	"context"
)

// SnapshotStore is a no-op implementation of models.SnapshotStore. Put and
// Delete succeed without storing anything and Get always reports no snapshot,
// so readers always take the cold path. Useful when a SnapshotStore is
// required but its behavior is irrelevant to the test.
type SnapshotStore struct{}

func (s *SnapshotStore) Put(
	_ context.Context,
	_ string,
	_ int64,
	_ []byte,
) error {
	return nil
}

func (s *SnapshotStore) Get(
	_ context.Context,
	_ string,
) ([]byte, bool, error) {
	return nil,
		false,
		nil
}

func (s *SnapshotStore) Delete(
	_ context.Context,
	_ string,
) error {
	return nil
}
