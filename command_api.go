package asynx

// CommandAPI groups the operations that change state or control the instance.
type CommandAPI[T any] interface {
	CommandSender[T]
	DeliveryControl[T]
	Lifecycle[T]
}
