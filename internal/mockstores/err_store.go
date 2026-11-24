package mockstores

import (
	"context"
	"github.com/char2cs/asynx/models"
)

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
	return models.Receipt{},
		false,
		e.Err
}

func (e *ErrStore) PendingDeliveries(
	context.Context,
	int,
) ([]models.Delivery, error) {
	return nil,
		e.Err
}

func (e *ErrStore) AckDelivery(
	context.Context,
	string,
	int64,
) error {
	return e.Err
}

func (e *ErrStore) Append(
	_ context.Context,
	_ string,
	_ int64,
	_ []byte,
) error {
	return e.Err
}

func (e *ErrStore) ReadFrom(
	_ context.Context,
	_ string,
	_ int64,
) ([][]byte, error) {
	return nil,
		e.Err
}

func (e *ErrStore) ReadRange(
	_ context.Context,
	_ string,
	_ int64,
	_ int64,
) ([][]byte, error) {
	return nil,
		e.Err
}

func (e *ErrStore) Delete(
	_ context.Context,
	_ string,
) error {
	return e.Err
}
