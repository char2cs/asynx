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
		go func() {
			defer finished.Done()
			start.Wait()
			results[i] = run(i)
		}()
	}

	start.Done()
	finished.Wait()
	return results
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

func concurrentSameVersion(
	t testing.TB,
	s models.Store,
) {
	const writers = 16
	ctx := context.Background()
	results := raceAll(
		writers,
		func(
			i int,
		) error {
			return s.Atomically(
				ctx,
				func(
					tx models.Tx,
				) error {
					return tx.Append(
						ctx,
						"s1",
						1,
						[]byte{byte(i)},
					)
				},
			)
		},
	)

	committed := 0
	for _, err := range results {
		switch {
		case err == nil:
			committed++
		case !errors.Is(
			err,
			models.ErrPipelineFailed,
		):
			t.Errorf(
				"losing writer got %v, want ErrPipelineFailed",
				err,
			)
		}
	}

	if committed != 1 {
		t.Fatalf(
			"%d writers committed version 1, want exactly one",
			committed,
		)
	}

	if blobs, _ := s.ReadFrom(
		ctx,
		"s1",
		1,
	); len(blobs) != 1 {
		t.Fatalf(
			"stream holds %d records, want 1",
			len(blobs),
		)
	}
}

func concurrentDistinctVersions(
	t testing.TB,
	s models.Store,
) {
	const writers = 20
	ctx := context.Background()
	results := raceAll(
		writers,
		func(
			i int,
		) error {
			v := int64(i + 1)
			return s.Atomically(
				ctx,
				func(
					tx models.Tx,
				) error {
					return errors.Join(
						tx.Append(
							ctx,
							"s1",
							v,
							[]byte{byte(v)},
						),
						tx.EnqueueDelivery(
							ctx,
							"s1",
							v,
						),
					)
				},
			)
		},
	)
	for _, err := range results {
		if err != nil {
			t.Fatalf(
				"a non-conflicting transaction failed: %v",
				err,
			)
		}
	}

	blobs, err := s.ReadFrom(
		ctx,
		"s1",
		1,
	)
	if err != nil ||
		len(blobs) != writers {
		t.Fatalf(
			"stream holds %d records (%v), want %d",
			len(blobs),
			err,
			writers,
		)
	}

	for i, blob := range blobs {
		if blob[0] != byte(i+1) {
			t.Fatalf(
				"record %d holds %d, want the stream in version order",
				i,
				blob[0],
			)
		}
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		100,
	); len(pending) != writers {
		t.Fatalf(
			"%d deliveries pending, want %d",
			len(pending),
			writers,
		)
	}
}

func concurrencyScenarios() map[string]scenario[models.Store] {
	return map[string]scenario[models.Store]{
		"ConcurrentAppendsOfOneVersionAdmitOne":        concurrentSameVersion,
		"ConcurrentAppendsOfDistinctVersionsAllCommit": concurrentDistinctVersions,
	}
}
