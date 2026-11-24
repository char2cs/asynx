package asynx

// Asynx is the event-sourced command and query facade for aggregate type T.
type Asynx[T any] interface {
	CommandAPI[T]
	QueryAPI[T]
}
