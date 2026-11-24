package delivery

import (
	"math/rand/v2"
	"time"
)

const jitterDivisor = 2

func jittered(
	delay time.Duration,
) time.Duration {
	half := delay / jitterDivisor
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
