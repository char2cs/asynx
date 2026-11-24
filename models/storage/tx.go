package storage

// Tx is the write side of a Store transaction. Every method joins the
// transaction opened by Store.Atomically: if the function passed to
// Atomically returns an error, every write made through the Tx is rolled back
// together, whichever of the groups below it belongs to.
//
// Tx is the union of three groups. TxStreamWriter writes event streams and
// the receipts of the commands that produced them, TxDeliveryWriter writes the
// delivery log, and TxNative exposes the store's own transaction handle. An
// event, its receipt and its delivery entry are written through one Tx so that
// none of them can be committed without the others.
//
// A Tx belongs to the call of Atomically that produced it. It must not be used
// after the function passed to Atomically returns, and it must not be shared
// with other goroutines. Every method honours its context: a context that is
// already done fails the call with the context's error and changes nothing.
type Tx interface {
	TxStreamWriter
	TxDeliveryWriter
	TxNative
}
