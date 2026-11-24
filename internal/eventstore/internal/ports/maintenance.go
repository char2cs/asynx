package ports

// Maintenance is the administrative side of an event store: removing an
// aggregate's data and reaching the replay, backing store and snapshot guard
// that recovery and Forget are built on. It is what the command processor needs
// beyond the ordinary read and write path.
type Maintenance[T any] interface {
	Erasing
	Inspecting[T]
}
