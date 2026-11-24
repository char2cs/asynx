package core

import (
	"context"
	"errors"
	"fmt"
	"github.com/char2cs/asynx/internal/firsterror"
	asynxmd "github.com/char2cs/asynx/models"
	"maps"
	"slices"
)

// Recover re-enqueues every unacknowledged delivery, in enqueue order, on the
// per-aggregate dispatcher, for the durable subscribers (live subscribers never
// see what happened while the process was down). It returns once they are all
// queued, not once they are handled: a handler that keeps failing must not stop
// the process from starting. It takes no aggregate lock, so it must run before
// any command is sent, as Build does: then a live event enqueues behind the
// recovered events of its aggregate in the same FIFO. Handlers run, retry on
// failure and acknowledge exactly as for live events.
//
// Deliveries whose event no longer exists are acknowledged and skipped. An
// aggregate whose event cannot be rebuilt (a corrupt record, an upcaster error)
// is parked with the error reported to the publish error handler, and the other
// aggregates are recovered regardless; ResumeDeliveries retries it. Delivery is
// at-least-once: an event whose handlers finished but whose ack was lost to a
// crash is delivered again.
func (p *Processor[T]) Recover(
	ctx context.Context,
	batch int,
) error {
	if p.dispatcher == nil {
		return nil
	}

	if batch <= 0 {
		batch = defaultRecoverBatch
	}

	all := func(
		string,
	) bool {
		return true
	}
	backlogs, order, err := p.pendingByAggregate(
		ctx,
		batch,
		all,
	)
	if err != nil {
		return err
	}

	recoverOne := func(
		agg string,
	) error {
		return p.recoverAggregate(
			ctx,
			agg,
			backlogs[agg],
		)
	}
	return firsterror.Of(
		order,
		recoverOne,
	)
}

func (p *Processor[T]) recoverAggregate(
	ctx context.Context,
	agg string,
	backlog []asynxmd.Delivery,
) error {
	undelivered, err := p.redeliver(
		ctx,
		agg,
		backlog,
	)
	if err == nil {
		return nil
	}

	if recoveryMustStop(
		ctx,
		err,
	) {
		return fmt.Errorf(
			"asynx: recovery: %w",
			err,
		)
	}

	p.dispatcher.Park(
		agg,
		err,
		undelivered...,
	)
	p.reportPublishError(
		ctx,
		asynxmd.Event[T]{AggregateID: agg, Version: backlog[0].Version},
		fmt.Errorf(
			"recovery: %w",
			err,
		),
	)
	return nil
}

const (
	defaultRecoverBatch = 100
	scanGrowthFactor    = 2
)

func recoveryMustStop(
	ctx context.Context,
	err error,
) bool {
	if ctx.Err() != nil {
		return true
	}

	return errors.Is(
		err,
		asynxmd.ErrDispatcherClosed,
	)
}

func (p *Processor[T]) scanPending(
	ctx context.Context,
	batch int,
	visit func(asynxmd.Delivery) error,
) error {
	return p.scanFrom(
		ctx,
		batch,
		make(map[asynxmd.Delivery]struct{}),
		visit,
	)
}

func (p *Processor[T]) scanFrom(
	ctx context.Context,
	limit int,
	seen map[asynxmd.Delivery]struct{},
	visit func(asynxmd.Delivery) error,
) error {
	pending, err := p.es.Store().PendingDeliveries(
		ctx,
		limit,
	)
	if err != nil {
		return fmt.Errorf(
			"asynx: recovery: %w",
			err,
		)
	}

	err = visitUnseen(
		pending,
		seen,
		visit,
	)
	if err != nil {
		return fmt.Errorf(
			"asynx: recovery: %w",
			err,
		)
	}

	if len(pending) < limit {
		return nil
	}

	return p.scanFrom(
		ctx,
		limit*scanGrowthFactor,
		seen,
		visit,
	)
}

func visitUnseen(
	pending []asynxmd.Delivery,
	seen map[asynxmd.Delivery]struct{},
	visit func(asynxmd.Delivery) error,
) error {
	isSeen := func(
		d asynxmd.Delivery,
	) bool {
		_, again := seen[d]
		seen[d] = struct{}{}
		return again
	}
	unseen := slices.DeleteFunc(
		slices.Clone(pending),
		isSeen,
	)
	return firsterror.Of(
		unseen,
		visit,
	)
}

func (p *Processor[T]) pendingByAggregate(
	ctx context.Context,
	batch int,
	want func(aggregateID string) bool,
) (backlogs map[string][]asynxmd.Delivery, order []string, err error) {
	backlogs = make(map[string][]asynxmd.Delivery)
	collect := func(
		d asynxmd.Delivery,
	) error {
		order = indexDelivery(
			backlogs,
			order,
			d,
			want,
		)
		return nil
	}
	err = p.scanPending(
		ctx,
		batch,
		collect,
	)
	return backlogs,
		order,
		err
}

func indexDelivery(
	backlogs map[string][]asynxmd.Delivery,
	order []string,
	d asynxmd.Delivery,
	want func(aggregateID string) bool,
) []string {
	if !want(d.AggregateID) {
		return order
	}

	if _, known := backlogs[d.AggregateID]; !known {
		order = append(
			order,
			d.AggregateID,
		)
	}

	backlogs[d.AggregateID] = append(
		backlogs[d.AggregateID],
		d,
	)
	return order
}

func (p *Processor[T]) redeliver(
	ctx context.Context,
	agg string,
	backlog []asynxmd.Delivery,
) (notEnqueued []int64, err error) {
	if len(backlog) == 0 {
		return nil,
			nil
	}

	vanished, err := p.enqueueBacklog(
		ctx,
		agg,
		backlog,
	)
	if err != nil {
		return slices.Sorted(maps.Keys(vanished)),
			err
	}

	return nil,
		p.ackVanished(
			ctx,
			agg,
			backlog,
			vanished,
		)
}

func (p *Processor[T]) enqueueBacklog(
	ctx context.Context,
	agg string,
	backlog []asynxmd.Delivery,
) (vanished map[int64]struct{}, err error) {
	vanished, lowest, highest := versionBounds(backlog)
	enqueuer := &backlogEnqueuer[T]{
		dispatcher: p.dispatcher,
		store:      p.es.Store(),
		aggregate:  agg,
		vanished:   vanished,
	}
	enqueue := func(
		_ context.Context,
		evt asynxmd.Event[T],
	) {
		enqueuer.enqueue(
			ctx,
			evt,
		)
	}
	replayErr := p.es.Replay(
		ctx,
		agg,
		lowest,
		highest,
		enqueue,
	)
	return vanished,
		errors.Join(
			replayErr,
			enqueuer.err,
		)
}

func versionBounds(
	backlog []asynxmd.Delivery,
) (versions map[int64]struct{}, lowest, highest int64) {
	versions = make(
		map[int64]struct{},
		len(backlog),
	)
	lowest, highest = backlog[0].Version, backlog[0].Version
	for _, d := range backlog {
		versions[d.Version] = struct{}{}
		lowest = min(
			lowest,
			d.Version,
		)
		highest = max(
			highest,
			d.Version,
		)
	}

	return versions,
		lowest,
		highest
}

func (p *Processor[T]) ackVanished(
	ctx context.Context,
	agg string,
	backlog []asynxmd.Delivery,
	vanished map[int64]struct{},
) error {
	ack := func(
		d asynxmd.Delivery,
	) error {
		return p.ackIfVanished(
			ctx,
			agg,
			d,
			vanished,
		)
	}
	return firsterror.Of(
		backlog,
		ack,
	)
}

func (p *Processor[T]) ackIfVanished(
	ctx context.Context,
	agg string,
	d asynxmd.Delivery,
	vanished map[int64]struct{},
) error {
	if _, isVanished := vanished[d.Version]; !isVanished {
		return nil
	}

	return p.es.Store().AckDelivery(
		ctx,
		agg,
		d.Version,
	)
}
