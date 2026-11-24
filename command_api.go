package asynx

// CommandAPI is the half of Asynx that changes state or controls the running
// instance: sending commands and forgetting aggregates (CommandSender),
// inspecting and resuming parked durable deliveries (DeliveryControl), and
// shutting the instance down (Lifecycle).
//
// Every operation here either writes to the event store or alters what the
// instance does next, so a read-only component should be given QueryAPI
// instead.
type CommandAPI[T any] interface {
	CommandSender[T]
	DeliveryControl[T]
	Lifecycle[T]
}
