package dispatchertest

import "sync"

type tally struct {
	mu     sync.Mutex
	wg     sync.WaitGroup
	failed map[string]bool
}

func (t *tally) run(
	name string,
	check scenario,
	h harness,
) {
	defer t.wg.Done()
	didFail := (&recorder{}).run(check, h)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failed[name] = didFail
}
