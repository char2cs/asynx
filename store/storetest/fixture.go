package storetest

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store/storetest/internal/write"
	"slices"
	"testing"
)

type fixture struct {
	t testing.TB
	s models.Store
}

func newFixture(
	t testing.TB,
	s models.Store,
) fixture {
	return fixture{t: t, s: s}
}

func (f fixture) commit(
	writes ...write.Op,
) {
	f.t.Helper()
	err := f.try(writes...)
	if err != nil {
		f.t.Fatalf(
			"Atomically: %v",
			err,
		)
	}
}

func (f fixture) try(
	writes ...write.Op,
) error {
	return f.s.Atomically(
		context.Background(),
		write.Transaction(writes...),
	)
}

func (f fixture) blobs(
	streamID string,
	from int64,
) [][]byte {
	f.t.Helper()
	blobs, err := f.s.ReadFrom(
		context.Background(),
		streamID,
		from,
	)
	if err != nil {
		f.t.Fatalf(
			"ReadFrom(%s, %d): %v",
			streamID,
			from,
			err,
		)
	}

	return blobs
}

func (f fixture) pending(
	limit int,
) []models.Delivery {
	f.t.Helper()
	pending, err := f.s.PendingDeliveries(
		context.Background(),
		limit,
	)
	if err != nil {
		f.t.Fatalf(
			"PendingDeliveries(%d): %v",
			limit,
			err,
		)
	}

	return pending
}

func (f fixture) receipt(
	commandID string,
) (r models.Receipt, found bool) {
	f.t.Helper()
	r, found, err := f.s.GetReceipt(
		context.Background(),
		commandID,
	)
	if err != nil {
		f.t.Fatalf(
			"GetReceipt(%s): %v",
			commandID,
			err,
		)
	}

	return r,
		found
}

func (f fixture) ack(
	aggregateID string,
	version int64,
) {
	f.t.Helper()
	err := f.s.AckDelivery(
		context.Background(),
		aggregateID,
		version,
	)
	if err != nil {
		f.t.Fatalf(
			"AckDelivery(%s, %d): %v",
			aggregateID,
			version,
			err,
		)
	}
}

func (f fixture) wantBlobsFrom(
	streamID string,
	from int64,
	want ...string,
) {
	f.t.Helper()
	got := asStrings(f.blobs(
		streamID,
		from,
	))
	if !slices.Equal(
		got,
		want,
	) {
		f.t.Fatalf(
			"ReadFrom(%s, %d) = %q, want %q",
			streamID,
			from,
			got,
			want,
		)
	}
}

func (f fixture) wantBlobsInRange(
	streamID string,
	from int64,
	count int64,
	want ...string,
) {
	f.t.Helper()
	blobs, err := f.s.ReadRange(
		context.Background(),
		streamID,
		from,
		count,
	)
	if err != nil {
		f.t.Fatalf(
			"ReadRange(%s, %d, %d): %v",
			streamID,
			from,
			count,
			err,
		)
	}

	if !slices.Equal(
		asStrings(blobs),
		want,
	) {
		f.t.Fatalf(
			"ReadRange(%s, %d, %d) = %q, want %q",
			streamID,
			from,
			count,
			blobs,
			want,
		)
	}
}

func (f fixture) wantPending(
	want ...models.Delivery,
) {
	f.t.Helper()
	got := f.pending(len(want) + 1)
	if !slices.Equal(
		got,
		want,
	) {
		f.t.Fatalf(
			"pending = %v, want %v",
			got,
			want,
		)
	}
}

func (f fixture) wantReceipt(
	commandID string,
	want models.Receipt,
) {
	f.t.Helper()
	got, found := f.receipt(commandID)
	if !found ||
		got != want {
		f.t.Fatalf(
			"GetReceipt(%s) = %+v, %v; want %+v",
			commandID,
			got,
			found,
			want,
		)
	}
}

func (f fixture) wantNoReceipt(
	commandID string,
) {
	f.t.Helper()
	if _, found := f.receipt(commandID); found {
		f.t.Errorf(
			"GetReceipt(%s) reported a receipt that was never committed",
			commandID,
		)
	}
}

func (f fixture) tryAtomically(
	ctx context.Context,
) error {
	const versionNotYetWritten = 2
	return f.s.Atomically(
		ctx,
		write.Transaction(write.Blob(
			"s1",
			versionNotYetWritten,
			[]byte("b"),
		)),
	)
}

func (f fixture) tryReadFrom(
	ctx context.Context,
) error {
	_, err := f.s.ReadFrom(
		ctx,
		"s1",
		1,
	)
	return err
}

func (f fixture) tryReadRange(
	ctx context.Context,
) error {
	_, err := f.s.ReadRange(
		ctx,
		"s1",
		1,
		1,
	)
	return err
}

func (f fixture) tryGetReceipt(
	ctx context.Context,
) error {
	_, _, err := f.s.GetReceipt(
		ctx,
		"cmd",
	)
	return err
}

func (f fixture) tryPendingDeliveries(
	ctx context.Context,
) error {
	_, err := f.s.PendingDeliveries(
		ctx,
		1,
	)
	return err
}

func (f fixture) tryAckDelivery(
	ctx context.Context,
) error {
	return f.s.AckDelivery(
		ctx,
		"s1",
		1,
	)
}

func (f fixture) wantOneWinner(
	results []error,
) {
	f.t.Helper()
	isCommitted := func(
		err error,
	) bool {
		return err == nil
	}
	isConflict := func(
		err error,
	) bool {
		return errors.Is(
			err,
			models.ErrPipelineFailed,
		)
	}

	losers := slices.DeleteFunc(
		slices.Clone(results),
		isCommitted,
	)
	if got := len(results) - len(losers); got != 1 {
		f.t.Fatalf(
			"%d writers committed version 1, want exactly one",
			got,
		)
	}

	unexpected := slices.DeleteFunc(
		losers,
		isConflict,
	)
	if len(unexpected) != 0 {
		f.t.Errorf(
			"losing writers got %v, want ErrPipelineFailed",
			errors.Join(unexpected...),
		)
	}
}

func (f fixture) wantAllCommitted(
	results []error,
) {
	f.t.Helper()
	err := errors.Join(results...)
	if err != nil {
		f.t.Fatalf(
			"transactions that do not conflict failed: %v",
			err,
		)
	}
}

func (f fixture) wantVersionsInOrder(
	streamID string,
	count int,
) {
	f.t.Helper()
	want := make(
		[]string,
		count,
	)
	for i := range count {
		want[i] = string([]byte{byte(i + 1)})
	}

	f.wantBlobsFrom(
		streamID,
		1,
		want...,
	)
}

func (f fixture) wantPendingCount(
	want int,
) {
	f.t.Helper()
	if got := len(f.pending(want + 1)); got != want {
		f.t.Fatalf(
			"%d deliveries pending, want %d",
			got,
			want,
		)
	}
}

func (f fixture) cancelledOutcomes(
	done context.Context,
) map[string]error {
	calls := map[string]func(context.Context) error{
		"Atomically":        f.tryAtomically,
		"ReadFrom":          f.tryReadFrom,
		"ReadRange":         f.tryReadRange,
		"GetReceipt":        f.tryGetReceipt,
		"PendingDeliveries": f.tryPendingDeliveries,
		"AckDelivery":       f.tryAckDelivery,
	}
	outcomes := make(
		map[string]error,
		len(calls),
	)
	for name, call := range calls {
		outcomes[name] = call(done)
	}

	return outcomes
}
