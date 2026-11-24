package models

// CommandMeta identifies a command and says whether it snapshots.
type CommandMeta interface {
	AggregateID() string
	EventName() string
	ShouldSnapshot() bool
}
