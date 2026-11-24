package exec

type outcome int

const (
	completed outcome = iota
	panicked
	timedOut
)
