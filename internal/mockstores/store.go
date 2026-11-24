package mockstores

import (
	"context"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/storage"
)

// Store is a no-op implementation of storage.Store. All operations succeed
// without storing or returning any data. Useful when a Store is required but
// its behavior is irrelevant to the test.
type Store struct{}

func (s *Store) tx() storage.Tx { return stubTx{appendFn: s.Append, deleteFn: s.Delete} }

// Atomically runs fn against a transaction that discards every write.
func (s *Store) Atomically(
	_ context.Context,
	fn func(storage.Tx) error,
) error {
	return fn(s.tx())
}

// GetReceipt reports that no receipt exists.
func (s *Store) GetReceipt(
	context.Context,
	string,
) (event.Receipt, bool, error) {
	return event.Receipt{},
		false,
		nil
}

// PendingDeliveries returns no pending deliveries.
func (s *Store) PendingDeliveries(
	context.Context,
	int,
) ([]event.Delivery, error) {
	return nil,
		nil
}

// AckDelivery does nothing and succeeds.
func (s *Store) AckDelivery(
	context.Context,
	string,
	int64,
) error {
	return nil
}

// Append discards the data and succeeds.
func (s *Store) Append(
	_ context.Context,
	_ string,
	_ int64,
	_ []byte,
) error {
	return nil
}

// ReadFrom returns no events.
func (s *Store) ReadFrom(
	_ context.Context,
	_ string,
	_ int64,
) ([][]byte, error) {
	return nil,
		nil
}

// ReadRange returns no events.
func (s *Store) ReadRange(
	_ context.Context,
	_ string,
	_ int64,
	_ int64,
) ([][]byte, error) {
	return nil,
		nil
}

// Delete does nothing and succeeds.
func (s *Store) Delete(
	_ context.Context,
	_ string,
) error {
	return nil
}
