package contracts

// Parking is the registry's control of parked aggregates: imposing a park,
// lifting it and reading it.
type Parking[J any] interface {
	ParkSetter[J]
	ParkLifter
	ParkReader
}
