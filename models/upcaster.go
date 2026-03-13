package models

import "context"

// Upcaster transforms raw patch bytes for a single schema version step.
// It receives the publishing context, the event name, and the JSON-encoded
// RFC 6902 patch array for the event at SchemaVersion v.
// It must return JSON-encoded patches compatible with SchemaVersion v+1.
//
// Upcasters must be stateless and safe for concurrent use.
type Upcaster func(
	ctx context.Context,
	eventName string,
	patches []byte,
) ([]byte, error)
