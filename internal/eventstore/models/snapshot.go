package models

// SnapshotBlob is stored in the snapshot stream.
// Version records the event version at which the snapshot was taken,
// so Reader knows exactly where to resume delta event loading.
// State is stored as an inline JSON object, decoded directly to T in a
// single json.Unmarshal call.
type SnapshotBlob[T any] struct {
	Version       int64 `json:"version"`
	SchemaVersion int   `json:"schema_version"`
	State         T     `json:"state"`
}
