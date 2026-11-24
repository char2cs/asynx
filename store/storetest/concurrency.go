package storetest

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/models"
	"sync"
	"testing"
)

func raceAll(
	n int,
	run func(i int) error,
) []error {
	results := make(
		[]error,
		n,
	)
	var start, finished sync.WaitGroup
	start.Add(1)
	for i := range n {
		finished.Add(1)
		go racer(
			&start,
			&finished,
			&results[i],
			run,
			i,
		)
	}

	start.Done()
	finished.Wait()
	return results
}

func racer(
	start *sync.WaitGroup,
	finished *sync.WaitGroup,
	out *error,
	run func(i int) error,
	i int,
) {
	defer finished.Done()
	start.Wait()
	*out = run(i)
}

func appendAndHold(
	ctx context.Context,
	tx models.Tx,
	appended chan<- struct{},
	release <-chan struct{},
) error {
	if err := tx.Append(
		ctx,
		"s1",
		1,
		[]byte("a"),
	); err != nil {
		return err
	}

	close(appended)
	<-release
	return nil
}

func sameVersionWriter(
	s models.Store,
) func(i int) error {
	return func(
		i int,
	) error {
		return s.Atomically(
			context.Background(),
			runOps(opAppend(
				"s1",
				1,
				[]byte{byte(i)},
			)),
		)
	}
}

func countCommitted(
	t testing.TB,
	results []error,
) int {
	committed := 0
	for _, err := range results {
		committed += tally(
			t,
			err,
		)
	}

	return committed
}

func tally(
	t testing.TB,
	err error,
) int {
	if err == nil {
		return 1
	}

	if !errors.Is(
		err,
		models.ErrPipelineFailed,
	) {
		t.Errorf(
			"losing writer got %v, want ErrPipelineFailed",
			err,
		)
	}

	return 0
}

func concurrentSameVersion(
	t testing.TB,
	s models.Store,
) {
	const writers = 16
	results := raceAll(
		writers,
		sameVersionWriter(s),
	)
	committed := countCommitted(
		t,
		results,
	)
	if committed != 1 {
		t.Fatalf(
			"%d writers committed version 1, want exactly one",
			committed,
		)
	}

	blobs := blobsOrFatal(
		t,
		s,
		"s1",
		1,
	)
	if len(blobs) != 1 {
		t.Fatalf(
			"stream holds %d records, want 1",
			len(blobs),
		)
	}
}

func distinctVersionWriter(
	s models.Store,
) func(i int) error {
	return func(
		i int,
	) error {
		v := int64(i + 1)
		return s.Atomically(
			context.Background(),
			runOps(
				opAppend(
					"s1",
					v,
					[]byte{byte(v)},
				),
				opEnqueue(models.Delivery{AggregateID: "s1", Version: v}),
			),
		)
	}
}

func wantVersionOrder(
	t testing.TB,
	blobs [][]byte,
) {
	for i, blob := range blobs {
		wantBlobAt(
			t,
			i,
			blob,
		)
	}
}

func wantBlobAt(
	t testing.TB,
	i int,
	blob []byte,
) {
	if blob[0] != byte(i+1) {
		t.Fatalf(
			"record %d holds %d, want the stream in version order",
			i,
			blob[0],
		)
	}
}

func wantNoErrors(
	t testing.TB,
	results []error,
) {
	for _, err := range results {
		wantNoError(
			t,
			err,
		)
	}
}

func wantNoError(
	t testing.TB,
	err error,
) {
	if err != nil {
		t.Fatalf(
			"a non-conflicting transaction failed: %v",
			err,
		)
	}
}

func wantPendingCount(
	t testing.TB,
	s models.Store,
	want int,
) {
	pending := pendingOrFatal(
		t,
		s,
		wideLimit,
	)
	if len(pending) != want {
		t.Fatalf(
			"%d deliveries pending, want %d",
			len(pending),
			want,
		)
	}
}

func concurrentDistinctVersions(
	t testing.TB,
	s models.Store,
) {
	const writers = 20
	results := raceAll(
		writers,
		distinctVersionWriter(s),
	)
	wantNoErrors(
		t,
		results,
	)
	blobs := blobsOrFatal(
		t,
		s,
		"s1",
		1,
	)
	if len(blobs) != writers {
		t.Fatalf(
			"stream holds %d records, want %d",
			len(blobs),
			writers,
		)
	}

	wantVersionOrder(
		t,
		blobs,
	)
	wantPendingCount(
		t,
		s,
		writers,
	)
}

func wantCancelled(
	t testing.TB,
	name string,
	err error,
) {
	t.Helper()
	if err == nil {
		t.Errorf(
			"%s succeeded on a cancelled context",
			name,
		)
	}
}

func cancelledReads(
	done context.Context,
	t testing.TB,
	s models.Store,
) {
	_, err := s.ReadFrom(
		done,
		"s1",
		1,
	)
	wantCancelled(
		t,
		"ReadFrom",
		err,
	)
	_, err = s.ReadRange(
		done,
		"s1",
		1,
		1,
	)
	wantCancelled(
		t,
		"ReadRange",
		err,
	)
	_, _, err = s.GetReceipt(
		done,
		"cmd",
	)
	wantCancelled(
		t,
		"GetReceipt",
		err,
	)
}

func cancelledDeliveries(
	done context.Context,
	t testing.TB,
	s models.Store,
) {
	_, err := s.PendingDeliveries(
		done,
		listLimit,
	)
	wantCancelled(
		t,
		"PendingDeliveries",
		err,
	)
	err = s.AckDelivery(
		done,
		"s1",
		1,
	)
	wantCancelled(
		t,
		"AckDelivery",
		err,
	)
}

func cancelledOps(
	done context.Context,
	t testing.TB,
	s models.Store,
) {
	cancelledReads(
		done,
		t,
		s,
	)
	cancelledDeliveries(
		done,
		t,
		s,
	)
}

func concurrencyScenarios() map[string]scenario[models.Store] {
	return map[string]scenario[models.Store]{
		"ConcurrentAppendsOfOneVersionAdmitOne":        concurrentSameVersion,
		"ConcurrentAppendsOfDistinctVersionsAllCommit": concurrentDistinctVersions,
	}
}
