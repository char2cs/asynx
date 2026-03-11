package models

// SnapshotBlob is stored in the snapshot stream.
// Version records the event version at which the snapshot was taken,
// so Reader knows exactly where to resume delta event loading.
type SnapshotBlob struct {
	Version       int64
	SchemaVersion int
	State         []byte
}
