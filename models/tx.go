package models

// Tx is the write side of a Store transaction. Every method joins the
// transaction opened by Store.Atomically: if the function passed to
// Atomically returns an error, all of them are rolled back together.
//
// Tx must not be used after the function passed to Atomically returns.
type Tx interface {
	TxStreamWriter
	TxDeliveryWriter
	TxReceiptWriter
}
