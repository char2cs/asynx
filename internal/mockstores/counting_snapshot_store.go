package mockstores

import (
	"context"
	"github.com/char2cs/asynx/models"
)

// CountingSnapshotStore wraps a models.SnapshotStore and records how often Get
// was called and how many bytes it returned. Use it to assert that a snapshot
// read does not scale with the number of snapshots ever written.
type CountingSnapshotStore struct {
	Inner    models.SnapshotStore
	GetCalls int
	GetBytes int
}

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

func (c *CountingSnapshotStore) Get(
	ctx context.Context,
	aggregateID string,
) ([]byte, bool, error) {
	data, found, err := c.Inner.Get(
		ctx,
		aggregateID,
	)
	c.GetCalls++
	c.GetBytes += len(data)

	return data,
		found,
		err
}

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
