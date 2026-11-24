package mockstores

import (
	"context"
	"github.com/char2cs/asynx/v2/models/storage"
)

// CountingSnapshotStore wraps a storage.SnapshotStore and records how often Get
// was called and how many bytes it returned. Use it to assert that a snapshot
// read does not scale with the number of snapshots ever written.
type CountingSnapshotStore struct {
	Inner    storage.SnapshotStore
	GetCalls int
	GetBytes int
}

// Put forwards to the inner store.
func (c *CountingSnapshotStore) Put(
	ctx context.Context,
	aggregateID string,
	version int64,
	data []byte,
) error {
	return c.Inner.Put(
		ctx,
		aggregateID,
		version,
		data,
	)
}

// Get forwards to the inner store and records the call and the bytes returned.
func (c *CountingSnapshotStore) Get(
	ctx context.Context,
	aggregateID string,
) ([]byte, bool, error) {
	data, found, err := c.Inner.Get(
		ctx,
		aggregateID,
	)
	c.record(len(data))

	return data,
		found,
		err
}

// Delete forwards to the inner store.
func (c *CountingSnapshotStore) Delete(
	ctx context.Context,
	aggregateID string,
) error {
	return c.Inner.Delete(
		ctx,
		aggregateID,
	)
}

// Reset zeroes the counters so a test can measure a single operation.
func (c *CountingSnapshotStore) Reset() {
	c.GetCalls, c.GetBytes = 0, 0
}

func (c *CountingSnapshotStore) record(
	n int,
) {
	c.GetCalls++
	c.GetBytes += n
}
