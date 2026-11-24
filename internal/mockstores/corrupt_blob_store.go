package mockstores

import (
	"context"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/storage"
)

// CorruptBlobStore is a Store implementation that returns a single
// unparseable blob from reads without returning an error.
// Used to trigger deserialize/unmarshal error paths in tests.
type CorruptBlobStore struct{}

func (c *CorruptBlobStore) tx() storage.Tx { return stubTx{appendFn: c.Append, deleteFn: c.Delete} }

// Atomically runs fn against a transaction that discards every write.
func (c *CorruptBlobStore) Atomically(
	_ context.Context,
	fn func(storage.Tx) error,
) error {
	return fn(c.tx())
}

// GetReceipt reports that no receipt exists.
func (c *CorruptBlobStore) GetReceipt(
	context.Context,
	string,
) (event.Receipt, bool, error) {
	return event.Receipt{},
		false,
		nil
}

// PendingDeliveries returns no pending deliveries.
func (c *CorruptBlobStore) PendingDeliveries(
	context.Context,
	int,
) ([]event.Delivery, error) {
	return nil,
		nil
}

// AckDelivery does nothing and succeeds.
func (c *CorruptBlobStore) AckDelivery(
	context.Context,
	string,
	int64,
) error {
	return nil
}

// Append discards the data and succeeds.
func (c *CorruptBlobStore) Append(
	_ context.Context,
	_ string,
	_ int64,
	_ []byte,
) error {
	return nil
}

// ReadFrom returns a single unparseable blob.
func (c *CorruptBlobStore) ReadFrom(
	_ context.Context,
	_ string,
	_ int64,
) ([][]byte, error) {
	return [][]byte{[]byte("!!!invalid")},
		nil
}

// ReadRange returns a single unparseable blob.
func (c *CorruptBlobStore) ReadRange(
	_ context.Context,
	_ string,
	_ int64,
	_ int64,
) ([][]byte, error) {
	return [][]byte{[]byte("!!!invalid")},
		nil
}

// Delete does nothing and succeeds.
func (c *CorruptBlobStore) Delete(
	_ context.Context,
	_ string,
) error {
	return nil
}
