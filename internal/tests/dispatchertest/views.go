package dispatchertest

import (
	"errors"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/char2cs/asynx/models"
)

func receive[V any](
	t testing.TB,
	patience time.Duration,
	ch <-chan V,
	what string,
) V {
	t.Helper()
	timer := time.NewTimer(patience)
	defer timer.Stop()
	select {
	case v := <-ch:
		return v
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", what)
	}
	var zero V
	return zero
}

func isClosed(
	ch <-chan struct{},
) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func parked(
	d models.Dispatcher[Aggregate],
) []string {
	return slices.Sorted(maps.Keys(d.Parks()))
}

func resume(
	d models.Dispatcher[Aggregate],
	aggregateID string,
) bool {
	generation, isParked := d.Parks()[aggregateID]
	if !isParked {
		return false
	}
	lifted, _ := d.ResumeUndelivered(aggregateID, generation)
	return lifted
}

func versionsOf(
	events []models.Event[Aggregate],
) []int64 {
	versions := make([]int64, 0, len(events))
	for _, e := range events {
		versions = append(versions, e.Version)
	}
	return versions
}

func sequence(
	n int64,
) []int64 {
	out := make([]int64, 0, n)
	for v := int64(1); v <= n; v++ {
		out = append(out, v)
	}
	return out
}

func byAggregate(
	events []models.Event[Aggregate],
) map[string][]int64 {
	out := map[string][]int64{}
	for _, e := range events {
		out[e.AggregateID] = append(out[e.AggregateID], e.Version)
	}
	return out
}

func distinct(
	versions []int64,
) int {
	seen := map[int64]struct{}{}
	for _, v := range versions {
		seen[v] = struct{}{}
	}
	return len(seen)
}

func without(
	entries []string,
	prefix string,
) []string {
	return slices.DeleteFunc(slices.Clone(entries), func(
		entry string,
	) bool {
		return len(entry) >= len(prefix) && entry[:len(prefix)] == prefix
	})
}

func unjoin(
	err error,
) []error {
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		return []error{err}
	}
	return joined.Unwrap()
}

func subscriberNames(
	err error,
) []string {
	var names []string
	for _, part := range unjoin(err) {
		names = appendSubscriber(names, part)
	}
	return names
}

func appendSubscriber(
	names []string,
	err error,
) []string {
	var sub *models.SubscriberError
	if !errors.As(err, &sub) {
		return names
	}
	return append(names, sub.Subscriber)
}
