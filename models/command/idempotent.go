package command

// Idempotent is optionally implemented by a Command that carries a
// client-minted id. When CommandID returns a non-empty string, asynx records a
// receipt for the command in the same transaction as its event: retrying the
// same id is a no-op that returns the original event, and a command that was
// rejected stays rejected.
type Idempotent interface {
	CommandID() string
}
