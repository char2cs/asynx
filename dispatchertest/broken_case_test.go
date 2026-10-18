package dispatchertest

import (
	"slices"
	"testing"

	"github.com/char2cs/asynx/models"
)

type brokenCase struct {
	wrap func(
		models.Dispatcher[Aggregate],
	) models.Dispatcher[Aggregate]
	flagged []string
}

func (c brokenCase) check(
	t *testing.T,
) {
	t.Parallel()
	failed := failures(harness{factory: brokenBy(c.wrap), patience: brokenPatience})
	t.Logf("failed scenarios: %v", failedNames(failed))
	missed := slices.DeleteFunc(slices.Clone(c.flagged), func(
		scenario string,
	) bool {
		return failed[scenario]
	})
	if len(missed) != 0 {
		t.Fatalf("the suite let it pass %v (failed: %v)", missed, failedNames(failed))
	}
}
