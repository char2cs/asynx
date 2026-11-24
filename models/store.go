package models

import (
	"context"
)

// Store is the durable backend for event streams, command receipts and the
// delivery log. Its contract is the foundation of asynx's guarantees; run
// store/storetest.Run against any implementation to check it.
//
// Reads come from StoreReader (streams and receipts) and DeliveryLog (the
// pending deliveries). Writes go through Atomically only, so an event can
// never be committed without the projections, receipt and delivery entry that
// belong to it. The one exception is DeliveryLog.AckDelivery, which removes an
// entry on its own once its handlers have finished.
//
// Guarantees every implementation must give:
//
//   - Atomicity: everything written through the Tx of one Atomically call
//     becomes visible together when fn returns nil, and not at all when fn
//     returns an error or panics.
//   - Uniqueness: a (streamID, version) pair is stored at most once, even
//     under concurrent writers; the loser of a race gets an error wrapping
//     ErrPipelineFailed. Optimistic concurrency in asynx relies on this alone.
//   - Ordering: reads return a stream in ascending version order whatever
//     order the versions were appended in, and deliveries come back in the
//     order they were enqueued.
//   - Context: every method that takes a context fails with the context's
//     error, changing nothing, when it is already done.
//   - Concurrency: all methods are safe for concurrent use by multiple
//     goroutines. Reads that overlap an open transaction must neither fail nor
//     deadlock it; whether they see its uncommitted writes is up to the
//     store's isolation level.
type Store interface {
	StoreReader
	DeliveryLog

	// Atomically runs fn in a transaction: it commits when fn returns nil and
	// rolls back when fn returns an error, which Atomically then returns, so
	// errors.Is and errors.As see through to it. A panic in fn must roll the
	// transaction back; the panic then keeps propagating to the caller. An
	// error from the commit itself, such as a version conflict detected only
	// then, is returned the same way and leaves nothing written.
	//
	// fn receives the transaction's Tx and must not call Atomically again or
	// keep the Tx once it returns. If ctx is already done, fn is not run and
	// the context's error is returned.
	Atomically(
		ctx context.Context,
		fn func(tx Tx) error,
	) error
}
