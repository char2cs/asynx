package asynx

import (
	"context"
	"github.com/char2cs/asynx/models"
	"sync"
	"sync/atomic"
)

type listener[T any] struct {
	bus   models.Bus[T]
	ch    chan models.Event[T]
	count int

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
	bus models.Bus[T],
	count int,
) *listener[T] {
	capacity := count
	if capacity <= 0 {
		capacity = 16
	}

	return &listener[T]{
		bus: bus,
		ch: make(
			chan models.Event[T],
			capacity,
		),
		count:      count,
		done:       make(chan struct{}),
		subIDReady: make(chan struct{}),
	}
}

func (l *listener[T]) handle(
	_ context.Context,
	evt models.Event[T],
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
	evt models.Event[T],
) {
	select {
	case <-l.done:
	case l.ch <- evt:
	}
}

func (l *listener[T]) sendBounded(
	evt models.Event[T],
) {
	withinCount := l.received.Add(1) <= int64(l.count)
	if !withinCount {
		return
	}

	if !l.deliver(evt) {
		return
	}

	go l.unsubscribeWhenPublished()
}

func (l *listener[T]) deliver(
	evt models.Event[T],
) (isLast bool) {
	l.sendMu.Lock()
	defer l.sendMu.Unlock()

	if l.closed.Load() {
		return false
	}

	l.ch <- evt
	l.delivered++
	isLast = l.delivered == int64(l.count)
	if isLast {
		l.closed.Store(true)
		l.closeChLocked()
	}

	return isLast
}

func (l *listener[T]) unsubscribeWhenPublished() {
	<-l.subIDReady
	if err := l.bus.Unsubscribe(l.subID); err != nil {
		return
	}
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

func (l *listener[T]) unsubscribe() {
	if !l.closed.CompareAndSwap(
		false,
		true,
	) {
		return
	}

	close(l.done)
	defer l.closeBoundedChannel()

	if err := l.bus.Unsubscribe(l.subID); err != nil {
		return
	}
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
