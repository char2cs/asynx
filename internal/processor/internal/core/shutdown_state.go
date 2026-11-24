package core

type shutdownState int

const (
	notShutDown shutdownState = iota
	shuttingDownNow
	shutdownFailed
	shutDown
)
