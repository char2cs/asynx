package models

// DispatcherParking groups the park controls and the park recovery of a Dispatcher.
type DispatcherParking interface {
	DispatcherParkControl
	DispatcherParkRecovery
}
