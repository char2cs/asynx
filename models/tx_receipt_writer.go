package models

import (
	"context"
)

// TxReceiptWriter stores command receipts and exposes the store's native transaction handle.
type TxReceiptWriter interface {
	// PutReceipt stores the receipt for commandID. It returns an error
	// wrapping ErrDuplicateReceipt when a receipt for commandID already exists.
	PutReceipt(
		ctx context.Context,
		commandID string,
		r Receipt,
	) error

	// Native returns the store's underlying transaction handle (for example a
	// *sql.Tx) so synchronous projections can write their own tables inside the
	// same transaction. In-memory stores return nil.
	Native() any
}
