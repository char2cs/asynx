package delivery

import (
	"math/rand/v2"
	"time"
)

func jittered(
	delay time.Duration,
) time.Duration {
	half := delay / 2
	if half <= 0 {
		return delay
	}
	return half + rand.N(half+1)
}

func isClosed(
	ch <-chan struct{},
) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
