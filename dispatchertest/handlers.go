package dispatchertest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/char2cs/asynx/models"
)

var (
	errPoison = errors.New("dispatchertest: poison")
	errDown   = errors.New("dispatchertest: down")
)

func always(
	string,
) bool {
	return true
}

func never(
	string,
) bool {
	return false
}

func onlyTestEvents(
	name string,
) bool {
	return name == eventName
}

func onlyForgetEvents(
	name string,
) bool {
	return name == models.ForgetEventName
}

func subscriber(
	handler models.DurableHandler[Aggregate],
) models.DurableSub[Aggregate] {
	return models.DurableSub[Aggregate]{Name: "sub", Match: always, Handler: handler}
}

func succeed(
	context.Context,
	models.Event[Aggregate],
) error {
	return nil
}

func fail(
	context.Context,
	models.Event[Aggregate],
) error {
	return errDown
}

func errorIf(
	cond bool,
	err error,
) error {
	if !cond {
		return nil
	}
	return err
}

func panicIf(
	cond bool,
	value any,
) {
	if !cond {
		return
	}
	panic(value)
}

func holdIf(
	cond bool,
	ch <-chan struct{},
) {
	if !cond {
		return
	}
	<-ch
}

func holdUpTo(
	ch <-chan struct{},
	patience time.Duration,
) {
	timer := time.NewTimer(patience)
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
	}
}

func flagIf(
	cond bool,
	flag *atomic.Bool,
) {
	if !cond {
		return
	}
	flag.Store(true)
}

func countIf(
	cond bool,
	counter *atomic.Int32,
) {
	if !cond {
		return
	}
	counter.Add(1)
}

func sendIf[V any](
	cond bool,
	ch chan<- V,
	value V,
) {
	if !cond {
		return
	}
	select {
	case ch <- value:
	default:
	}
}

func firstTime(
	once *sync.Once,
) bool {
	first := false
	once.Do(func() { first = true })
	return first
}

func holdFirst(
	once *sync.Once,
	entered chan<- struct{},
	release <-chan struct{},
) {
	once.Do(func() {
		close(entered)
		<-release
	})
}
