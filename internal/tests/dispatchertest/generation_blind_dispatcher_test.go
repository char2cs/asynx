package dispatchertest

import "github.com/char2cs/asynx/models"

type generationBlindDispatcher struct {
	models.Dispatcher[Aggregate]
}

func ignoreGeneration(
	inner models.Dispatcher[Aggregate],
) models.Dispatcher[Aggregate] {
	return generationBlindDispatcher{Dispatcher: inner}
}

func (d generationBlindDispatcher) ResumeUndelivered(
	aggregateID string,
	_ uint64,
) (bool, []int64) {
	return d.Dispatcher.ResumeUndelivered(aggregateID, d.Parks()[aggregateID])
}
