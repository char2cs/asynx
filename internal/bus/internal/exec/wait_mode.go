package exec

type waitMode int

const (
	abandonAfterTimeout waitMode = iota
	waitForHandler
)
