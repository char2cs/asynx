package asynx

import (
	"context"
	"fmt"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/pubsub"
	"sync"
	"sync/atomic"
)

type listener[T any] struct {
	bus     pubsub.Bus[T]
	ch      chan event.Event[T]
	count   int
	onError func(error)

	done       chan struct{}
	subIDReady chan struct{}
	received   atomic.Int64
	closed     atomic.Bool

	sendMu    sync.Mutex
	delivered int64
	chClosed  bool
	subID     string
}

func newListener[T any](
	bus pubsub.Bus[T],
	count int,
	onError func(error),
) *listener[T] {
	capacity := count
	if capacity <= 0 {
		capacity = 16
	}

	return &listener[T]{
		bus: bus,
		ch: make(
			chan event.Event[T],
			capacity,
		),
		count:      count,
		onError:    onError,
		done:       make(chan struct{}),
		subIDReady: make(chan struct{}),
	}
}

func (l *listener[T]) handle(
	_ context.Context,
	evt event.Event[T],
) {
	if l.closed.Load() {
		return
	}

	if l.count <= 0 {
		l.sendUnbounded(evt)
		return
	}

	l.sendBounded(evt)
}

func (l *listener[T]) sendUnbounded(
	evt event.Event[T],
) {
	select {
	case <-l.done:
	case l.ch <- evt:
	}
}

func (l *listener[T]) sendBounded(
	evt event.Event[T],
) {
	withinCount := l.received.Add(1) <= int64(l.count)
	if !withinCount {
		return
	}

	l.deliver(evt)
}

func (l *listener[T]) deliver(
	evt event.Event[T],
) {
	l.sendMu.Lock()
	defer l.sendMu.Unlock()

	if l.closed.Load() {
		return
	}

	l.ch <- evt
	l.delivered++
	if l.delivered < int64(l.count) {
		return
	}

	l.closed.Store(true)
	l.closeChLocked()
	go l.unsubscribeWhenPublished()
}

func (l *listener[T]) unsubscribeWhenPublished() {
	<-l.subIDReady
	l.report(l.removeSubscription())
}

func (l *listener[T]) removeSubscription() error {
	err := l.bus.Unsubscribe(l.subID)
	if err != nil {
		return fmt.Errorf(
			"asynx: unsubscribe listener subscription %s: %w",
			l.subID,
			err,
		)
	}

	return nil
}

func (l *listener[T]) report(
	err error,
) {
	if err == nil ||
		l.onError == nil {
		return
	}

	l.onError(err)
}

func (l *listener[T]) closeChLocked() {
	if l.chClosed {
		return
	}

	l.chClosed = true
	close(l.ch)
}

func (l *listener[T]) publishSubscription(
	id string,
) {
	l.sendMu.Lock()
	l.subID = id
	l.sendMu.Unlock()
	close(l.subIDReady)
}

func (l *listener[T]) unsubscribe() error {
	if !l.closed.CompareAndSwap(
		false,
		true,
	) {
		return nil
	}

	close(l.done)
	defer l.closeBoundedChannel()

	return l.removeSubscription()
}

func (l *listener[T]) release() {
	l.report(l.unsubscribe())
}

func (l *listener[T]) closeBoundedChannel() {
	isBounded := l.count > 0
	if !isBounded {
		return
	}

	l.sendMu.Lock()
	l.closeChLocked()
	l.sendMu.Unlock()
}
