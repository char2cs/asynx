package dispatchertest

import (
	"context"

	"github.com/char2cs/asynx/models"
)

func acceptingDeadLetters(
	cfg models.DispatcherConfig[Aggregate],
) (models.Dispatcher[Aggregate], error) {
	cfg.DeadLetter = swallowRefusals(cfg.DeadLetter)
	return defaultFactory(cfg)
}

func swallowRefusals(
	deadLetter models.DeadLetterHandler[Aggregate],
) models.DeadLetterHandler[Aggregate] {
	if deadLetter == nil {
		return nil
	}
	return func(
		ctx context.Context,
		e models.Event[Aggregate],
		err error,
	) error {
		_ = deadLetter(ctx, e, err)
		return nil
	}
}
