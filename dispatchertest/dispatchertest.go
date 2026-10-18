// Package dispatchertest is a conformance suite for models.Dispatcher
// implementations, as storetest is for stores. It checks the contract written
// on models.Dispatcher (per-aggregate order, durable retry and ack, dead
// letters, parks and their generations, supersede, tickets and unrun answers,
// interrupt and close, InHandler, and surviving panics in every handler) and
// nothing an implementation is free to choose. Run it from a test in the
// package that provides the dispatcher:
//
//	func TestConformance(t *testing.T) {
//		dispatchertest.Run(t, func(
//			cfg models.DispatcherConfig[dispatchertest.Aggregate],
//		) (models.Dispatcher[dispatchertest.Aggregate], error) {
//			return mydispatcher.New(cfg), nil
//		})
//	}
package dispatchertest

import (
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/char2cs/asynx/models"
)

const patience = 5 * time.Second

// Run executes the full conformance suite against dispatchers built by
// factory, one subtest per scenario. Each scenario builds its own dispatchers
// from the factory with the DispatcherConfig it needs (a recording bus, its
// durable subscribers, its handlers, a fast retry backoff, an attempt cap) and
// closes them when it ends. The factory must return a fresh, independent
// dispatcher on every call; a factory error fails the scenario. Every wait is
// bounded, so a dispatcher that loses an event fails instead of hanging.
func Run(
	t *testing.T,
	factory models.DispatcherFactory[Aggregate],
) {
	t.Helper()
	h := harness{factory: factory, patience: patience}
	all := scenarios()
	for _, name := range slices.Sorted(maps.Keys(all)) {
		t.Run(name, h.subtest(all[name]))
	}
}

func scenarios() map[string]scenario {
	all := map[string]scenario{}
	maps.Copy(all, orderingScenarios())
	maps.Copy(all, durableScenarios())
	maps.Copy(all, parkingScenarios())
	maps.Copy(all, ticketScenarios())
	maps.Copy(all, lifecycleScenarios())
	return all
}
