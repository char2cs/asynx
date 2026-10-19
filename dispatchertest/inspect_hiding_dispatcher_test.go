package dispatchertest

import (
	"testing"

	"github.com/char2cs/asynx/models"
)

type inspectHidingDispatcher struct {
	models.Dispatcher[Aggregate]
}

func hideParksFromInspect(
	inner models.Dispatcher[Aggregate],
) models.Dispatcher[Aggregate] {
	return inspectHidingDispatcher{Dispatcher: inner}
}

func (d inspectHidingDispatcher) Inspect(
	aggregateID string,
) models.DeliveryStatus {
	status := d.Dispatcher.Inspect(aggregateID)
	status.Parked = false
	return status
}

func TestSuite_FlagsADispatcherWhoseInspectHidesItsParks(t *testing.T) {
	brokenCase{hideParksFromInspect, []string{
		"TestInspect_ReportsParkGenerationCauseAndUndelivered",
		"TestInspect_ParkedAgainKeepsTheGenerationAndTheFirstCause",
		"TestInspect_ALaterCauseReplacesANilOne",
		"TestInspect_GenerationChangesWhenAParkIsImposedAgain",
		"TestInspect_IsClearAfterTheParkLifts",
		"TestInspect_GiveUpCauseNamesTheLastHandlerError",
		"TestInspect_AckFailureCauseIsTheAckError",
		"TestInspect_InterruptCauseIsDispatcherClosed",
		"TestInspect_ADurableEventRefusedByTheParkIsListedUndelivered",
	}}.check(t)
}
