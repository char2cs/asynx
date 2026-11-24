package mockstores

import (
	"context"
	"github.com/char2cs/asynx/models"
)

// ErrStore is a Store implementation that returns Err for every operation.
type ErrStore struct{ Err error }

func (e *ErrStore) tx() models.Tx { return stubTx{appendFn: e.Append, deleteFn: e.Delete} }

// Atomically runs fn against a transaction whose writes return Err.
func (e *ErrStore) Atomically(
	_ context.Context,
	fn func(models.Tx) error,
) error {
	return fn(e.tx())
}

// GetReceipt reports no receipt and returns Err on every call, whatever the
// command ID.
func (e *ErrStore) GetReceipt(
	context.Context,
	string,
) (models.Receipt, bool, error) {
	return models.Receipt{},
		false,
		e.Err
}

// PendingDeliveries lists nothing and returns Err on every call, whatever the
// limit.
func (e *ErrStore) PendingDeliveries(
	context.Context,
	int,
) ([]models.Delivery, error) {
	return nil,
		e.Err
}

// AckDelivery acknowledges nothing and returns Err on every call, whatever the
// aggregate and version.
func (e *ErrStore) AckDelivery(
	context.Context,
	string,
	int64,
) error {
	return e.Err
}

// Append stores nothing and returns Err on every call. Writes made through
// the transaction handed to Atomically reach this method, so they fail the
// same way.
func (e *ErrStore) Append(
	_ context.Context,
	_ string,
	_ int64,
	_ []byte,
) error {
	return e.Err
}

// ReadFrom reads nothing and returns Err on every call, whatever the stream
// and starting version.
func (e *ErrStore) ReadFrom(
	_ context.Context,
	_ string,
	_ int64,
) ([][]byte, error) {
	return nil,
		e.Err
}

// ReadRange reads nothing and returns Err on every call, whatever the stream,
// starting version and count.
func (e *ErrStore) ReadRange(
	_ context.Context,
	_ string,
	_ int64,
	_ int64,
) ([][]byte, error) {
	return nil,
		e.Err
}

// Delete removes nothing and returns Err on every call. Stream deletions made
// through the transaction handed to Atomically reach this method, so they
// fail the same way.
func (e *ErrStore) Delete(
	_ context.Context,
	_ string,
) error {
	return e.Err
}
