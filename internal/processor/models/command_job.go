package models

// CommandJob is a queued envelope waiting for its aggregate to be free.
type CommandJob[T any] struct {
	Envelope *CommandEnvelope[T]
}
