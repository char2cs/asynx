package models

import (
	"encoding/json"
	"time"
)

// InternalEvent is the storage representation of an event.
// Patches holds a JSON-encoded RFC 6902 patch array (old → new state).
// The full aggregate state is never stored — it is reconstructed by
// replaying patches on top of the seed state (or latest snapshot).
type InternalEvent struct {
	ID            string
	EventName     string
	Version       int64
	SchemaVersion int
	OccurredAt    time.Time
	Patches       json.RawMessage
}
