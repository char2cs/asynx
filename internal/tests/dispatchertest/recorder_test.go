package dispatchertest

import (
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
)

type recorder struct {
	testing.TB
	failed   atomic.Bool
	mu       sync.Mutex
	cleanups []func()
}

func (r *recorder) Helper() {}

func (r *recorder) Errorf(
	string,
	...any,
) {
	r.failed.Store(true)
}

func (r *recorder) Error(
	...any,
) {
	r.failed.Store(true)
}

func (r *recorder) Fatalf(
	string,
	...any,
) {
	r.failed.Store(true)
	runtime.Goexit()
}

func (r *recorder) Fatal(
	...any,
) {
	r.failed.Store(true)
	runtime.Goexit()
}

func (r *recorder) Cleanup(
	fn func(),
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanups = append(r.cleanups, fn)
}

func (r *recorder) cleanup() {
	r.mu.Lock()
	cleanups := slices.Clone(r.cleanups)
	r.mu.Unlock()
	slices.Reverse(cleanups)
	for _, fn := range cleanups {
		fn()
	}
}

func fails(
	check func(
		testing.TB,
	),
) bool {
	return (&recorder{}).run(func(
		t testing.TB,
		_ harness,
	) {
		check(t)
	}, harness{})
}

func (r *recorder) run(
	check scenario,
	h harness,
) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer r.cleanup()
		check(r, h)
	}()
	<-done
	return r.failed.Load()
}
