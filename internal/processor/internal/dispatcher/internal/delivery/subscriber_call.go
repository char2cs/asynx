package delivery

import (
	"context"
	"fmt"
	"time"

	"github.com/char2cs/asynx/v2/models/failure"
	"github.com/char2cs/asynx/v2/models/subscription"

	"github.com/char2cs/asynx/v2/internal/processor/internal/dispatcher/internal/contracts"
)

type subscriberCall[T any] struct {
	deliverer *durableDeliverer[T]
	job       contracts.Job[T]
	sub       subscription.Durable[T]
	attempt   int
}

func (c *subscriberCall[T]) run(
	ctx context.Context,
) error {
	if c.sub.Timeout <= 0 {
		return c.guarded(ctx)
	}

	result := make(
		chan error,
		1,
	)
	go c.deliverInto(
		ctx,
		result,
	)
	timer := time.NewTimer(c.sub.Timeout)
	defer timer.Stop()
	select {
	case err := <-result:
		return err
	case <-timer.C:
		c.reportTimeout(ctx)
		return <-result
	}
}

func (c *subscriberCall[T]) deliverInto(
	ctx context.Context,
	result chan<- error,
) {
	result <- c.guarded(ctx)
}

func (c *subscriberCall[T]) guarded(
	ctx context.Context,
) (err error) {
	defer c.recoverPanic(
		ctx,
		&err,
	)
	return c.sub.Handler(
		ctx,
		c.job.Event(),
	)
}

func (c *subscriberCall[T]) recoverPanic(
	ctx context.Context,
	err *error,
) {
	recovered := recover()
	if recovered == nil {
		return
	}

	c.deliverer.reports.Panic(
		ctx,
		c.job,
		recovered,
	)
	*err = fmt.Errorf(
		"%w: %v",
		failure.ErrHandlerPanic,
		recovered,
	)
}

func (c *subscriberCall[T]) reportTimeout(
	ctx context.Context,
) {
	timedOut := &failure.SubscriberError{
		Subscriber: c.sub.Name,
		Err: fmt.Errorf(
			"%w after %v",
			failure.ErrHandlerTimeout,
			c.sub.Timeout,
		),
	}
	c.deliverer.reports.DeliveryError(
		ctx,
		c.job,
		timedOut,
		c.attempt,
	)
}
