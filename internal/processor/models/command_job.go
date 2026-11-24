package models

// CommandJob is the shard-internal record of an accepted envelope, which a
// worker serves at once or parks in the mailbox of its aggregate until the
// command ahead of it finishes. It exists so the shard queues envelopes
// without exposing its bookkeeping to senders.
type CommandJob[T any] struct {
	Envelope *CommandEnvelope[T]
}
