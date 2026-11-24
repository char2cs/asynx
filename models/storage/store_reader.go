package storage

import (
	"context"

	"github.com/char2cs/asynx/models/event"
)

// StoreReader reads event streams and command receipts. It sees what
// committed transactions wrote through TxStreamWriter, and may or may not see
// the uncommitted writes of an open transaction, depending on the store's
// isolation level. All methods are safe for concurrent use and return the
// context's error, with no result, when ctx is already done.
type StoreReader interface {
	// ReadFrom returns the blobs of streamID with version >= fromVersion, in
	// ascending version order, whatever order they were appended in. A stream
	// that does not exist, or whose versions all lie below fromVersion,
	// yields an empty result and no error.
	ReadFrom(
		ctx context.Context,
		streamID string,
		fromVersion int64,
	) ([][]byte, error)

	// ReadRange returns at most count blobs of streamID starting at
	// fromVersion, in ascending version order: the window ReadFrom would
	// return, cut to count entries. A count larger than what is available
	// returns everything available; a count of zero returns nothing. A stream
	// that does not exist yields an empty result and no error.
	ReadRange(
		ctx context.Context,
		streamID string,
		fromVersion int64,
		count int64,
	) ([][]byte, error)

	// GetReceipt returns the receipt stored for commandID; found is false when
	// none exists, which is not an error. Only committed receipts are found:
	// one written by a rolled-back transaction never is.
	GetReceipt(
		ctx context.Context,
		commandID string,
	) (r event.Receipt, found bool, err error)
}
