package api

import (
	"context"
)

// Recoverer redelivers what is pending after a failure or a restart.
type Recoverer interface {
	// Resume lifts the park of an aggregate and redelivers what is pending.
	Resume(
		ctx context.Context,
		batch int,
	) error
	// Recover redelivers every pending durable delivery.
	Recover(
		ctx context.Context,
		batch int,
	) error
}
