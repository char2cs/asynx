package models

type CommandJob[T any] struct {
	Envelope *CommandEnvelope[T]
}
