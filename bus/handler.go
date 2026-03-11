package bus

import (
	"fmt"
	"os"
	"time"

	"github.com/char2cs/asynx"
)

type handlerJob[T any] struct {
	handler         func(asynx.Event[T])
	fallbackHandler func(asynx.Event[T])
	timeout         time.Duration
	event           asynx.Event[T]
}

func (b *ChannelBus[T]) executeHandler(job *handlerJob[T]) {
	defer b.inFlightWg.Done()

	defer func() {
		r := recover()
		if r == nil {
			return
		}

		b.onHandlerPanic(r, job)
		if job.fallbackHandler == nil {
			return
		}

		b.executeWithTimeout(job.fallbackHandler, job.event, job.timeout)
	}()

	b.executeWithTimeout(job.handler, job.event, job.timeout)
}

func (b *ChannelBus[T]) executeWithTimeout(
	handler func(asynx.Event[T]),
	event asynx.Event[T],
	timeout time.Duration,
) {
	if timeout == 0 {
		handler(event)
		return
	}

	done := make(chan struct{})

	go func() {
		defer func() {
			if r := recover(); r != nil {
				close(done)
			}
		}()
		handler(event)
		close(done)
	}()

	// Use NewTimer so Stop() can release the timer before it fires.
	// time.After would leak the timer for the full duration when <-done wins.
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		b.onHandlerTimeout(event, timeout)
	}
}

func (b *ChannelBus[T]) onHandlerPanic(panicValue any, job *handlerJob[T]) {
	fmt.Fprintf(
		os.Stderr,
		"asynx: projection handler panicked for event %q: %v\n",
		job.event.EventName,
		panicValue,
	)
}

func (b *ChannelBus[T]) onHandlerTimeout(event asynx.Event[T], timeout time.Duration) {
	fmt.Fprintf(
		os.Stderr,
		"asynx: projection handler timed out after %v for event %q\n",
		timeout,
		event.EventName,
	)
}
