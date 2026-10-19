package delivery

import "time"

type retrySettings struct {
	initial     time.Duration
	max         time.Duration
	maxAttempts int
}
