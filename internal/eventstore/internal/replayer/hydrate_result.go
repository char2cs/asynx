package replayer

// HydrateResult is returned by Hydrate. When DidUpcast is true, LastVersion
// holds the version at which the state was sealed — the caller should write
// an auto-snapshot using State and LastVersion.
type HydrateResult[T any] struct {
	State       T
	DidUpcast   bool
	LastVersion int64
}
