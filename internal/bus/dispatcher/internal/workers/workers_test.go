package workers

import (
	"testing"
	"time"
)

func TestNew_KeepsItsCollaboratorsAndStartsWithNoWorker(t *testing.T) {
	f := newFixture(t, 3*time.Second)
	pool := f.pool.(*aggregatePool[int])

	if pool.bus != f.bus || pool.registry != f.registry || pool.deliverer != f.deliverer || pool.reports != f.reports {
		t.Fatal("New did not keep its collaborators")
	}
	if pool.idleTimeout != 3*time.Second {
		t.Fatalf("idle timeout = %v, want 3s", pool.idleTimeout)
	}

	f.pool.Wait()
}
