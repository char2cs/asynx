package inmemory

type inHandlerKey[T any] struct {
	d *inMemoryDispatcher[T]
}
