package storetest

import (
	"errors"
	"github.com/char2cs/asynx/models"
	"maps"
	"testing"
)

// Factory returns a fresh, empty store for one subtest.
type Factory func(
	t *testing.T,
) models.Store

var errBoom = errors.New("storetest: boom")

// Run executes the full conformance suite against stores built by newStore.
func Run(
	t *testing.T,
	newStore Factory,
) {
	t.Helper()

	tests := storeScenarios()

	for name, fn := range tests {
		t.Run(
			name,
			func(
				t *testing.T,
			) {
				fn(
					t,
					newStore(t),
				)
			},
		)
	}
}

func storeScenarios() map[string]scenario[models.Store] {
	scenarios := map[string]scenario[models.Store]{}
	for _, group := range []map[string]scenario[models.Store]{
		appendScenarios(),
		concurrencyScenarios(),
		transactionScenarios(),
		deliveryScenarios(),
	} {
		maps.Copy(
			scenarios,
			group,
		)
	}

	return scenarios
}
