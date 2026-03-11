package exec

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

// HandlerJob carries all the data needed to execute one subscription handler
// for a single event.
type HandlerJob[T any] struct {
	Handler         asynxmd.ProjectionHandler[T]
	FallbackHandler asynxmd.ProjectionHandler[T]
	Timeout         time.Duration
	Event           asynxmd.Event[T]
	Ctx             context.Context

	// Optional per-job callback overrides. When nil, the package-level
	// OnHandlerPanic / OnHandlerTimeout functions are used instead.
	OnPanic   asynxmd.PanicHandler[T]
	OnTimeout asynxmd.TimeoutHandler[T]
}

// ExecuteHandler runs job.Handler (via ExecuteWithTimeout), recovers from any
// panic, and optionally calls job.FallbackHandler. It calls wg.Done() when it
// returns. Intended to be called as a goroutine.
func ExecuteHandler[T any](wg *sync.WaitGroup, job *HandlerJob[T]) {
	defer wg.Done()

	// Build resolved callbacks — job-level overrides the package default.
	panicFn := func(v any) {
		if job.OnPanic != nil {
			job.OnPanic(job.Ctx, job.Event, v)
		} else {
			OnHandlerPanic(v, job)
		}
	}
	timeoutFn := func() {
		if job.OnTimeout != nil {
			job.OnTimeout(job.Ctx, job.Event, job.Timeout)
		} else {
			OnHandlerTimeout(job.Event, job.Timeout)
		}
	}

	defer func() {
		r := recover()
		if r == nil {
			return
		}

		panicFn(r)
		if job.FallbackHandler == nil {
			return
		}

		// Wrap the fallback call in its own recover so a second panic (Bug 1)
		// never escapes this defer function and crash the process.
		func() {
			defer func() {
				if r2 := recover(); r2 != nil {
					panicFn(r2)
				}
			}()
			ExecuteWithTimeout(job.Ctx, job.FallbackHandler, job.Event, job.Timeout, panicFn, timeoutFn)
		}()
	}()

	ExecuteWithTimeout(job.Ctx, job.Handler, job.Event, job.Timeout, panicFn, timeoutFn)
}

// ExecuteWithTimeout calls handler inline when timeout is zero, or in a
// goroutine with a deadline otherwise. Panics inside the goroutine are
// recovered and reported via onPanic (if non-nil) before closing the done
// channel, so they never propagate.
func ExecuteWithTimeout[T any](
	ctx context.Context,
	handler asynxmd.ProjectionHandler[T],
	event asynxmd.Event[T],
	timeout time.Duration,
	onPanic func(any),
	onTimeout func(),
) {
	if timeout == 0 {
		handler(ctx, event)
		return
	}

	done := make(chan struct{})

	go func() {
		defer func() {
			if r := recover(); r != nil {
				if onPanic != nil {
					onPanic(r)
				}
				close(done)
			}
		}()
		handler(ctx, event)
		close(done)
	}()

	// Use NewTimer so Stop() can release the timer before it fires.
	// time.After would leak the timer for the full duration when <-done wins.
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		if onTimeout != nil {
			onTimeout()
		}
	}
}

func OnHandlerPanic[T any](
	panicValue any,
	job *HandlerJob[T],
) {
	fmt.Fprintf(
		os.Stderr,
		"asynx: projection handler panicked for event %q: %v\n",
		job.Event.EventName,
		panicValue,
	)
}

func OnHandlerTimeout[T any](
	event asynxmd.Event[T],
	timeout time.Duration,
) {
	fmt.Fprintf(
		os.Stderr,
		"asynx: projection handler timed out after %v for event %q\n",
		timeout,
		event.EventName,
	)
}
