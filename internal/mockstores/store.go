package mockstores

import (
	"context"
	"github.com/char2cs/asynx/models"
)

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
	return models.Receipt{},
		false,
		nil
}

func (s *Store) PendingDeliveries(
	context.Context,
	int,
) ([]models.Delivery, error) {
	return nil,
		nil
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
	return nil,
		nil
}

func (s *Store) ReadRange(
	_ context.Context,
	_ string,
	_ int64,
	_ int64,
) ([][]byte, error) {
	return nil,
		nil
}

func (s *Store) Delete(
	_ context.Context,
	_ string,
) error {
	return nil
}
