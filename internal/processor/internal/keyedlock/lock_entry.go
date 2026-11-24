package keyedlock

type lockEntry struct {
	ch   chan struct{}
	refs int
}
