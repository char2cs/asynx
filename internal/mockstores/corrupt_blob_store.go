package mockstores

import (
	"context"
	"github.com/char2cs/asynx/models"
)

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
	return models.Receipt{},
		false,
		nil
}

func (c *CorruptBlobStore) PendingDeliveries(
	context.Context,
	int,
) ([]models.Delivery, error) {
	return nil,
		nil
}

func (c *CorruptBlobStore) AckDelivery(
	context.Context,
	string,
	int64,
) error {
	return nil
}

func (c *CorruptBlobStore) Append(
	_ context.Context,
	_ string,
	_ int64,
	_ []byte,
) error {
	return nil
}

func (c *CorruptBlobStore) ReadFrom(
	_ context.Context,
	_ string,
	_ int64,
) ([][]byte, error) {
	return [][]byte{[]byte("!!!invalid")},
		nil
}

func (c *CorruptBlobStore) ReadRange(
	_ context.Context,
	_ string,
	_ int64,
	_ int64,
) ([][]byte, error) {
	return [][]byte{[]byte("!!!invalid")},
		nil
}

func (c *CorruptBlobStore) Delete(
	_ context.Context,
	_ string,
) error {
	return nil
}
