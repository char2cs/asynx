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
	ID            string          `json:"id"`
	EventName     string          `json:"event_name"`
	Version       int64           `json:"version"`
	SchemaVersion int             `json:"schema_version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Patches       json.RawMessage `json:"patches"`
}
