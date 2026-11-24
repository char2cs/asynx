package ports

// Maintenance covers the administrative side of an event store.
type Maintenance[T any] interface {
	Erasing
	Inspecting[T]
}
