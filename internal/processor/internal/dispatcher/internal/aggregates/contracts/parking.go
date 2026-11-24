package contracts

import "github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/aggregates/contracts/inspection"

// Parking is the registry's control of parked aggregates: imposing a park,
// lifting it and reading it.
type Parking[J any] interface {
	ParkSetter[J]
	ParkLifter
	inspection.ParkReader
}
