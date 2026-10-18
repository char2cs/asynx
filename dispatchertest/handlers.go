package dispatchertest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

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
