package models

import (
	"context"
)

// StoreReader reads event streams and command receipts.
type StoreReader interface {
	// ReadFrom returns the blobs of streamID with version >= fromVersion, in
	// version order.
	ReadFrom(
		ctx context.Context,
		streamID string,
		fromVersion int64,
	) ([][]byte, error)

	// ReadRange returns at most count blobs of streamID starting at
	// fromVersion, in version order.
	ReadRange(
		ctx context.Context,
		streamID string,
		fromVersion int64,
		count int64,
	) ([][]byte, error)

	// GetReceipt returns the receipt for commandID; found is false when none
	// exists.
	GetReceipt(
		ctx context.Context,
		commandID string,
	) (r Receipt, found bool, err error)
}
