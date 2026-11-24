package aggregates

// Settler settles and completes an item's delivery.
type Settler interface {
	// MarkUnrun settles the item as one whose handlers never ran, with err as
	// the reason. It is called under the registry mutex and must not call into
	// the Registry; a second settle is a no-op.
	MarkUnrun(
		err error,
	)
	// Complete ends the item's delivery with err. It may be called under the
	// registry mutex and must not call into the Registry; a second completion is
	// a no-op.
	Complete(
		err error,
	)
}
