package inmemory

import "context"

type liveCancel struct {
	owner  *cancels
	cancel context.CancelFunc
}

func newLiveCancel(
	owner *cancels,
	cancel context.CancelFunc,
) *liveCancel {
	return &liveCancel{
		owner:  owner,
		cancel: cancel,
	}
}

func (l *liveCancel) release() {
	l.owner.remove(l)
	l.cancel()
}
