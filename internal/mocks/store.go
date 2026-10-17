package mocks

import (
	"context"

	"github.com/char2cs/asynx/models"
)

// stubTx is the Tx handed out by the mock stores: Append and DeleteStream
// delegate to the owning store, everything else succeeds without effect.
type stubTx struct {
	appendFn func(context.Context, string, int64, []byte) error
	deleteFn func(context.Context, string) error
}

func (t stubTx) Append(
	ctx context.Context,
	id string,
	v int64,
	d []byte,
) error {
	return t.appendFn(ctx, id, v, d)
}

func (t stubTx) DeleteStream(
	ctx context.Context,
	id string,
) error {
	return t.deleteFn(ctx, id)
}

func (stubTx) DropDeliveries(
	context.Context,
	string,
) error {
	return nil
}
func (stubTx) PutReceipt(
	context.Context,
	string,
	models.Receipt,
) error {
	return nil
}
func (stubTx) EnqueueDelivery(
	context.Context,
	string,
	int64,
) error {
	return nil
}
func (stubTx) Native() any { return nil }

// Store is a no-op implementation of models.Store. All operations succeed
// without storing or returning any data. Useful when a Store is required but
// its behavior is irrelevant to the test.
type Store struct{}

func (s *Store) tx() models.Tx { return stubTx{appendFn: s.Append, deleteFn: s.Delete} }

func (s *Store) Atomically(
	_ context.Context,
	fn func(models.Tx) error,
) error {
	return fn(s.tx())
}

func (s *Store) GetReceipt(
	context.Context,
	string,
) (models.Receipt, bool, error) {
	return models.Receipt{}, false, nil
}

func (s *Store) PendingDeliveries(
	context.Context,
	int,
) ([]models.Delivery, error) {
	return nil, nil
}

func (s *Store) AckDelivery(
	context.Context,
	string,
	int64,
) error {
	return nil
}

func (s *Store) Append(
	_ context.Context,
	_ string,
	_ int64,
	_ []byte,
) error {
	return nil
}

func (s *Store) ReadFrom(
	_ context.Context,
	_ string,
	_ int64,
) ([][]byte, error) {
	return nil, nil
}

func (s *Store) ReadRange(
	_ context.Context,
	_ string,
	_ int64,
	_ int64,
) ([][]byte, error) {
	return nil, nil
}

func (s *Store) Delete(_ context.Context, _ string) error {
	return nil
}

// ErrStore is a Store implementation that returns Err for every operation.
type ErrStore struct{ Err error }

func (e *ErrStore) tx() models.Tx { return stubTx{appendFn: e.Append, deleteFn: e.Delete} }

func (e *ErrStore) Atomically(
	_ context.Context,
	fn func(models.Tx) error,
) error {
	return fn(e.tx())
}

func (e *ErrStore) GetReceipt(
	context.Context,
	string,
) (models.Receipt, bool, error) {
	return models.Receipt{}, false, e.Err
}

func (e *ErrStore) PendingDeliveries(
	context.Context,
	int,
) ([]models.Delivery, error) {
	return nil, e.Err
}

func (e *ErrStore) AckDelivery(
	context.Context,
	string,
	int64,
) error {
	return e.Err
}

func (e *ErrStore) Append(_ context.Context, _ string, _ int64, _ []byte) error {
	return e.Err
}

func (e *ErrStore) ReadFrom(_ context.Context, _ string, _ int64) ([][]byte, error) {
	return nil, e.Err
}

func (e *ErrStore) ReadRange(_ context.Context, _ string, _ int64, _ int64) ([][]byte, error) {
	return nil, e.Err
}

func (e *ErrStore) Delete(_ context.Context, _ string) error {
	return e.Err
}

// CorruptBlobStore is a Store implementation that returns a single
// unparseable blob from reads without returning an error.
// Used to trigger deserialize/unmarshal error paths in tests.
type CorruptBlobStore struct{}

func (c *CorruptBlobStore) tx() models.Tx { return stubTx{appendFn: c.Append, deleteFn: c.Delete} }

func (c *CorruptBlobStore) Atomically(
	_ context.Context,
	fn func(models.Tx) error,
) error {
	return fn(c.tx())
}

func (c *CorruptBlobStore) GetReceipt(
	context.Context,
	string,
) (models.Receipt, bool, error) {
	return models.Receipt{}, false, nil
}

func (c *CorruptBlobStore) PendingDeliveries(
	context.Context,
	int,
) ([]models.Delivery, error) {
	return nil, nil
}

func (c *CorruptBlobStore) AckDelivery(
	context.Context,
	string,
	int64,
) error {
	return nil
}

func (c *CorruptBlobStore) Append(_ context.Context, _ string, _ int64, _ []byte) error {
	return nil
}

func (c *CorruptBlobStore) ReadFrom(_ context.Context, _ string, _ int64) ([][]byte, error) {
	return [][]byte{[]byte("!!!invalid")}, nil
}

func (c *CorruptBlobStore) ReadRange(_ context.Context, _ string, _ int64, _ int64) ([][]byte, error) {
	return [][]byte{[]byte("!!!invalid")}, nil
}

func (c *CorruptBlobStore) Delete(_ context.Context, _ string) error {
	return nil
}

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
	return nil, false, nil
}

func (s *SnapshotStore) Delete(
	_ context.Context,
	_ string,
) error {
	return nil
}

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
	return c.Inner.Put(ctx, aggregateID, version, data)
}

func (c *CountingSnapshotStore) Get(
	ctx context.Context,
	aggregateID string,
) ([]byte, bool, error) {
	data, found, err := c.Inner.Get(ctx, aggregateID)
	c.GetCalls++
	c.GetBytes += len(data)

	return data, found, err
}

func (c *CountingSnapshotStore) Delete(
	ctx context.Context,
	aggregateID string,
) error {
	return c.Inner.Delete(ctx, aggregateID)
}

// Reset zeroes the counters so a test can measure a single operation.
func (c *CountingSnapshotStore) Reset() {
	c.GetCalls, c.GetBytes = 0, 0
}
