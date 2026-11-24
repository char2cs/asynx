package storage

import (
	"context"

	"github.com/char2cs/asynx/models/event"
)

// TxStreamWriter writes event streams and command receipts inside a
// transaction. It mirrors the read side, StoreReader: whatever is written here
// is read back there once the transaction has committed.
//
// Nothing written through a TxStreamWriter is visible as committed until
// Atomically returns nil. Whether a reader outside the transaction can see the
// uncommitted writes before that is up to the store's isolation level; they
// must never survive a rollback.
type TxStreamWriter interface {
	// Append stores data at (streamID, version). It enforces
	// (streamID, version) uniqueness, the sole coordination mechanism for
	// correctness across concurrent writers, and returns an error wrapping
	// failure.ErrPipelineFailed on a conflict, whether the version was written by this
	// transaction, by an earlier committed one or by a concurrent one. The
	// conflict may be reported by Append itself or, when the store detects it
	// only at commit, by Atomically; either way the transaction does not
	// commit. Versions need not be appended in order: reads return a stream
	// in ascending version order.
	Append(
		ctx context.Context,
		streamID string,
		version int64,
		data []byte,
	) error

	// DeleteStream removes every record of streamID. It is idempotent:
	// deleting a stream that does not exist is not an error. Other streams are
	// untouched, and the stream may be appended to again afterwards, starting
	// from any version.
	DeleteStream(
		ctx context.Context,
		streamID string,
	) error

	// PutReceipt stores the receipt for commandID. It returns an error
	// wrapping failure.ErrDuplicateReceipt when a receipt for commandID already
	// exists, whether it was stored by this transaction or by an earlier
	// committed one; the existing receipt is left unchanged. Receipts are
	// immutable: there is no update and no delete. The receipt becomes
	// visible to StoreReader.GetReceipt when the transaction commits, and
	// never if it rolls back.
	PutReceipt(
		ctx context.Context,
		commandID string,
		r event.Receipt,
	) error
}
