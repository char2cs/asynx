package workers

import (
	"testing"
	"time"
)

func TestNew_KeepsItsCollaboratorsAndStartsWithNoWorker(t *testing.T) {
	f := newFixture(t, 3*time.Second)
	pool := f.pool.(*aggregatePool[int])

	collaborators := [4]any{pool.bus, pool.registry, pool.deliverer, pool.reports}
	if collaborators != [4]any{f.bus, f.registry, f.deliverer, f.reports} {
		t.Fatal("New did not keep its collaborators")
	}
	if pool.idleTimeout != 3*time.Second {
		t.Fatalf("idle timeout = %v, want 3s", pool.idleTimeout)
	}

	f.pool.Wait()
}
