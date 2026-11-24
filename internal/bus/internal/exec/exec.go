package exec

import (
	"context"
	"fmt"
	asynxmd "github.com/char2cs/asynx/models"
	"os"
	"sync"
	"time"
)

// Handler runs one subscription handler for one event and reports how it
// ended. It is meant to be started as its own goroutine by the bus, which
// reserves a slot on wg beforehand; Handler calls wg.Done exactly once, when
// everything it started for the job has finished, so a bus can count in-flight
// work with the group.
//
// job.Handler runs inline on the calling goroutine when job.Timeout is zero,
// and on a separate goroutine bounded by the timeout otherwise. A panic in the
// handler never escapes Handler, with or without a timeout: it is reported
// through job.OnPanic (the default OnHandlerPanic when nil). A timeout is
// reported through job.OnTimeout (the default OnHandlerTimeout when nil); the
// handler goroutine is then abandoned, unless job.WaitAfterTimeout is set, in
// which case Handler still waits for it to finish before returning.
//
// job.FallbackHandler, when set, runs once when the primary handler failed:
// when it panicked, with or without a timeout, or when it exceeded
// job.Timeout. The fallback gets the same context, event, timeout and wait
// mode. After a timeout it starts right after the timeout is reported. With
// job.WaitAfterTimeout it starts only once the primary has finished, so the two
// never overlap and a hung primary holds the job exactly as it does without a
// fallback; otherwise the primary is abandoned and the fallback runs at once,
// possibly while the primary is still running. A panic in the fallback is reported through the
// same callback and ends the job, so there is no second fallback, and a panic
// the primary raises after its timeout is reported but triggers nothing more.
//
// The panic and timeout callbacks run on goroutines nobody recovers, so a
// panic raised inside one is contained, never propagated: a misbehaving
// observer must not take the process down. With no further observer to tell,
// the contained panic is written to stderr as one line, which is the only trace
// of it. This is the documented guarantee for WithPanicHandler and
// WithTimeoutHandler.
func Handler[T any](
	ctx context.Context,
	wg *sync.WaitGroup,
	job *HandlerJob[T],
) {
	defer wg.Done()

	panicFn := panicNotifier(
		ctx,
		job,
	)
	timeoutFn := timeoutNotifier(
		ctx,
		job,
	)
	fallback := fallbackRunner(
		ctx,
		job,
		panicFn,
		timeoutFn,
	)
	result := runWithDeadline(
		ctx,
		job.Handler,
		job.Event,
		job.Timeout,
		panicFn,
		timeoutFn,
		job.waitMode(),
	)
	if result == panicked {
		fallback()
	}

	if result == timedOut {
		fallback()
	}
}

func containObserverPanic() {
	recovered := recover()
	if recovered == nil {
		return
	}

	fmt.Fprintf(
		os.Stderr,
		"asynx: a panic observer panicked and was contained: %v\n",
		recovered,
	)
}

func panicNotifier[T any](
	ctx context.Context,
	job *HandlerJob[T],
) func(any) {
	return func(
		v any,
	) {
		defer containObserverPanic()
		notifyPanic(
			ctx,
			job,
			v,
		)
	}
}

func notifyPanic[T any](
	ctx context.Context,
	job *HandlerJob[T],
	v any,
) {
	if job.OnPanic == nil {
		OnHandlerPanic(
			v,
			job,
		)
		return
	}

	job.OnPanic(
		ctx,
		job.Event,
		v,
	)
}

func timeoutNotifier[T any](
	ctx context.Context,
	job *HandlerJob[T],
) func() {
	return func() {
		defer containObserverPanic()
		notifyTimeout(
			ctx,
			job,
		)
	}
}

func notifyTimeout[T any](
	ctx context.Context,
	job *HandlerJob[T],
) {
	if job.OnTimeout == nil {
		OnHandlerTimeout(
			job.Event,
			job.Timeout,
		)
		return
	}

	job.OnTimeout(
		ctx,
		job.Event,
		job.Timeout,
	)
}

func fallbackRunner[T any](
	ctx context.Context,
	job *HandlerJob[T],
	panicFn func(any),
	timeoutFn func(),
) func() {
	return func() {
		runFallback(
			ctx,
			job,
			panicFn,
			timeoutFn,
		)
	}
}

func runFallback[T any](
	ctx context.Context,
	job *HandlerJob[T],
	panicFn func(any),
	timeoutFn func(),
) {
	if job.FallbackHandler == nil {
		return
	}

	runWithDeadline(
		ctx,
		job.FallbackHandler,
		job.Event,
		job.Timeout,
		panicFn,
		timeoutFn,
		job.waitMode(),
	)
}

// WithTimeout calls handler for event inline when timeout is zero, or on its
// own goroutine with a deadline otherwise, and returns once the handler is done
// or the deadline passed, whichever is first. When the deadline passes first,
// onTimeout (if non-nil) is called and the handler goroutine is abandoned: it
// keeps running and its eventual result is ignored.
//
// A panic in the handler is recovered on its goroutine and handed to onPanic
// before the handler counts as done, so it never propagates to the caller. When
// onPanic is nil the recovered value is dropped; Handler always passes a
// notifier, so only direct callers can hit that. A panic raised by onPanic
// itself is contained and written to stderr. With a zero timeout the handler runs on the
// caller's goroutine and a panic propagates to the caller.
func WithTimeout[T any](
	ctx context.Context,
	handler asynxmd.ProjectionHandler[T],
	event asynxmd.Event[T],
	timeout time.Duration,
	onPanic func(any),
	onTimeout func(),
) {
	if timeout == 0 {
		handler(
			ctx,
			event,
		)
		return
	}

	runWithDeadline(
		ctx,
		handler,
		event,
		timeout,
		onPanic,
		onTimeout,
		abandonAfterTimeout,
	)
}

func runWithDeadline[T any](
	ctx context.Context,
	handler asynxmd.ProjectionHandler[T],
	event asynxmd.Event[T],
	timeout time.Duration,
	onPanic func(any),
	onTimeout func(),
	mode waitMode,
) outcome {
	if timeout == 0 {
		return callRecovering(
			ctx,
			handler,
			event,
			onPanic,
		)
	}

	done := make(
		chan outcome,
		1,
	)
	go runRecovering(
		ctx,
		handler,
		event,
		onPanic,
		done,
	)

	return awaitOutcome(
		done,
		timeout,
		onTimeout,
		mode,
	)
}

func awaitOutcome(
	done <-chan outcome,
	timeout time.Duration,
	onTimeout func(),
	mode waitMode,
) outcome {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case result := <-done:
		return result
	case <-timer.C:
		handleTimeout(
			onTimeout,
			mode,
			done,
		)
		return timedOut
	}
}

func runRecovering[T any](
	ctx context.Context,
	handler asynxmd.ProjectionHandler[T],
	event asynxmd.Event[T],
	onPanic func(any),
	done chan<- outcome,
) {
	done <- callRecovering(
		ctx,
		handler,
		event,
		onPanic,
	)
}

func callRecovering[T any](
	ctx context.Context,
	handler asynxmd.ProjectionHandler[T],
	event asynxmd.Event[T],
	onPanic func(any),
) (result outcome) {
	defer reportRecoveredPanic(
		onPanic,
		&result,
	)
	handler(
		ctx,
		event,
	)

	return completed
}

func reportRecoveredPanic(
	onPanic func(any),
	result *outcome,
) {
	recovered := recover()
	if recovered == nil {
		return
	}

	*result = panicked
	if onPanic == nil {
		return
	}

	defer containObserverPanic()
	onPanic(recovered)
}

func handleTimeout(
	onTimeout func(),
	mode waitMode,
	done <-chan outcome,
) {
	if onTimeout != nil {
		onTimeout()
	}

	if mode == waitForHandler {
		<-done
	}
}

// OnHandlerPanic is the panic callback a job without its own OnPanic falls
// back to. It writes one line naming the event and the recovered value to
// stderr and returns; it never fails and does not re-raise the panic. Handler
// calls it on the goroutine that recovered the panic.
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

// OnHandlerTimeout is the timeout callback a job without its own OnTimeout
// falls back to. It writes one line naming the event and the configured timeout
// to stderr and returns. Handler calls it from the goroutine waiting on the
// handler, at the moment the deadline passes.
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

func (j *HandlerJob[T]) waitMode() waitMode {
	if j.WaitAfterTimeout {
		return waitForHandler
	}

	return abandonAfterTimeout
}
