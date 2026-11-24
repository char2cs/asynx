package inmemory

type inHandlerKey[T any] struct {
	d *dispatcher[T]
}
