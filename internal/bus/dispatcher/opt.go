package dispatcher

import "time"

// Opt is a functional option for the in-memory knobs of the default
// dispatcher, which models.DispatcherConfig does not carry. Options are
// applied by New before the subsystems are built.
type Opt[T any] func(
	d *dispatcher[T],
)

// WithIdleTimeout sets how long a per-aggregate worker goroutine waits for new
// events before it retires; the aggregate's next event starts a new one. The
// default is five seconds. Zero or negative values are ignored.
func WithIdleTimeout[T any](
	t time.Duration,
) Opt[T] {
	if t <= 0 {
		return func(
			*dispatcher[T],
		) {
		}
	}
	return func(
		d *dispatcher[T],
	) {
		d.idleTimeout = t
	}
}
