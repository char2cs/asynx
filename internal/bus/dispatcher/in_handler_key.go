package dispatcher

type inHandlerKey[T any] struct {
	d *dispatcher[T]
}
