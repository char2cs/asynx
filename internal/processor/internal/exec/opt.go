package exec

// Opt configures a CommandExecutor.
type Opt[T any] func(
	*CommandExecutor[T],
)
