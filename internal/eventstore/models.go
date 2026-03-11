package eventstore

import (
	"encoding/json"
	"time"
)

// internalEvent is the storage representation of an event.
// Patches holds a JSON-encoded RFC 6902 patch array (old → new state).
// The full aggregate state is never stored — it is reconstructed by
// replaying patches on top of the seed state (or latest snapshot).
type internalEvent struct {
	ID            string
	EventName     string
	Version       int64
	SchemaVersion int
	OccurredAt    time.Time
	Patches       json.RawMessage
}

// snapshotBlob is stored in the snapshot stream.
// Version records the event version at which the snapshot was taken,
// so Reader knows exactly where to resume delta event loading.
type snapshotBlob struct {
	Version       int64
	SchemaVersion int
	State         []byte
}
