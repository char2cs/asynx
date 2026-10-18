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

	// WaitAfterTimeout makes a timed-out handler still be waited for: the
	// timeout is reported but ExecuteHandler returns only once the handler has
	// finished. The dispatcher path needs this so the next event of an
	// aggregate never starts while the previous one is still being handled.
	WaitAfterTimeout bool
}

// ExecuteHandler runs job.Handler (via ExecuteWithTimeout), recovers from any
// panic, and optionally calls job.FallbackHandler. It calls wg.Done() when it
// returns. Intended to be called as a goroutine. The panic and timeout callbacks
// run in goroutines nobody recovers, so a panic inside one is swallowed: a
// misbehaving observer must not take the process down.
func ExecuteHandler[T any](
	wg *sync.WaitGroup,
	job *HandlerJob[T],
) {
	defer wg.Done()

	panicFn := panicCallback(job)
	timeoutFn := timeoutCallback(job)
	defer fallBackOnPanic(job, panicFn, timeoutFn)

	executeWithTimeout(job.Ctx, job.Handler, job.Event, job.Timeout, panicFn, timeoutFn, job.WaitAfterTimeout)
}

func panicCallback[T any](job *HandlerJob[T]) func(any) {
	return func(v any) {
		defer func() { _ = recover() }()
		if job.OnPanic == nil {
			OnHandlerPanic(v, job)
			return
		}
		job.OnPanic(job.Ctx, job.Event, v)
	}
}

func timeoutCallback[T any](job *HandlerJob[T]) func() {
	return func() {
		defer func() { _ = recover() }()
		if job.OnTimeout == nil {
			OnHandlerTimeout(job.Event, job.Timeout)
			return
		}
		job.OnTimeout(job.Ctx, job.Event, job.Timeout)
	}
}

func fallBackOnPanic[T any](
	job *HandlerJob[T],
	panicFn func(any),
	timeoutFn func(),
) {
	recovered := recover()
	if recovered == nil {
		return
	}

	panicFn(recovered)
	if job.FallbackHandler == nil {
		return
	}
	runFallback(job, panicFn, timeoutFn)
}

func runFallback[T any](
	job *HandlerJob[T],
	panicFn func(any),
	timeoutFn func(),
) {
	defer func() {
		if recovered := recover(); recovered != nil {
			panicFn(recovered)
		}
	}()
	executeWithTimeout(job.Ctx, job.FallbackHandler, job.Event, job.Timeout, panicFn, timeoutFn, job.WaitAfterTimeout)
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
	executeWithTimeout(ctx, handler, event, timeout, onPanic, onTimeout, false)
}

// executeWithTimeout is ExecuteWithTimeout with the option to keep waiting for
// the handler after the timeout has been reported.
func executeWithTimeout[T any](
	ctx context.Context,
	handler asynxmd.ProjectionHandler[T],
	event asynxmd.Event[T],
	timeout time.Duration,
	onPanic func(any),
	onTimeout func(),
	waitAfterTimeout bool,
) {
	if timeout == 0 {
		handler(ctx, event)
		return
	}

	done := make(chan struct{})
	go runGuarded(ctx, handler, event, onPanic, done)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		afterTimeout(onTimeout, waitAfterTimeout, done)
	}
}

func runGuarded[T any](
	ctx context.Context,
	handler asynxmd.ProjectionHandler[T],
	event asynxmd.Event[T],
	onPanic func(any),
	done chan<- struct{},
) {
	defer close(done)
	defer reportPanic(onPanic)
	handler(ctx, event)
}

func reportPanic(onPanic func(any)) {
	recovered := recover()
	if recovered == nil || onPanic == nil {
		return
	}
	defer func() { _ = recover() }()
	onPanic(recovered)
}

func afterTimeout(
	onTimeout func(),
	waitAfterTimeout bool,
	done <-chan struct{},
) {
	if onTimeout != nil {
		onTimeout()
	}
	if waitAfterTimeout {
		<-done
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
