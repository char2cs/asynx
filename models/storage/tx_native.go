package storage

// TxNative gives synchronous projections access to the store's own
// transaction, so they can write their tables atomically with the event.
type TxNative interface {
	// Native returns the store's underlying transaction handle (for example a
	// *sql.Tx), so that a synchronous projection can write its own tables
	// inside the same transaction and commit or roll back together with the
	// event. The handle is valid until the function passed to Atomically
	// returns and must not be committed, rolled back or closed by the caller.
	//
	// A store with no underlying handle, such as the in-memory store, returns
	// nil; callers that need the handle must be prepared for nil. Calling
	// Native has no effect on the transaction.
	Native() any
}
