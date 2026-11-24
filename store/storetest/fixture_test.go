package storetest

import (
	"github.com/char2cs/asynx/v2/models/event"
	"github.com/char2cs/asynx/v2/store"
	"github.com/char2cs/asynx/v2/store/storetest/internal/write"
	"testing"
)

func TestFixture_CommitThenReadsBackWhatWasWritten(
	t *testing.T,
) {
	f := newFixture(
		t,
		store.New(),
	)
	f.commit(
		write.Blob(
			"s1",
			1,
			[]byte("a"),
		),
		write.Delivery(deliveryOf(
			"s1",
			1,
		)),
		write.Receipt(
			"cmd",
			event.Receipt{Accepted: true},
		),
	)

	f.wantBlobsFrom(
		"s1",
		1,
		"a",
	)
	f.wantPending(deliveryOf(
		"s1",
		1,
	))
	f.wantReceipt(
		"cmd",
		event.Receipt{Accepted: true},
	)
	f.wantNoReceipt("other")
}

func TestFixture_TryReportsTheFirstFailureAndStopsThere(
	t *testing.T,
) {
	f := newFixture(
		t,
		store.New(),
	)

	err := f.try(
		write.Abort(errBoom),
		write.Blob(
			"s1",
			1,
			[]byte("never"),
		),
	)
	if err != errBoom {
		t.Fatalf(
			"try = %v, want the first write's error",
			err,
		)
	}

	f.wantBlobsFrom(
		"s1",
		1,
	)
}

func TestFixture_FailuresAreReportedThroughTheTestingHandle(
	t *testing.T,
) {
	r := &recorder{}
	f := newFixture(
		r,
		store.New(),
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.wantBlobsFrom(
			"s1",
			1,
			"a",
		)
	}()
	<-done

	if !r.failed.Load() {
		t.Fatal("an assertion that does not hold did not fail the test")
	}
}
