package reader

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	"github.com/char2cs/asynx/store"
)

func benchEventBlob(version int64, patches json.RawMessage) []byte {
	evt := esmodels.InternalEvent{
		ID:            "bench",
		EventName:     "Updated",
		Version:       version,
		SchemaVersion: 1,
		OccurredAt:    time.Now().UTC(),
		Patches:       patches,
	}
	b, err := json.Marshal(evt)
	if err != nil {
		panic(err)
	}
	return b
}

func benchSnapshotBlob(version int64, state order) []byte {
	snap := esmodels.SnapshotBlob[order]{
		Version:       version,
		SchemaVersion: 1,
		State:         state,
	}
	b, err := json.Marshal(snap)
	if err != nil {
		panic(err)
	}
	return b
}

func populateEvents(b *testing.B, es store.Memory, startVersion, n int) {
	b.Helper()
	patch := json.RawMessage(`[{"op":"replace","path":"/Status","value":"x"}]`)
	ctx := context.Background()
	for i := 0; i < n; i++ {
		v := int64(startVersion + i)
		if err := es.Append(ctx, "events:agg1", v, benchEventBlob(v, patch)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet_WarmPath(b *testing.B) {
	for _, tc := range []struct {
		name   string
		deltas int
	}{
		{"deltas=1", 1},
		{"deltas=10", 10},
		{"deltas=1_000_000", 1_000_000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			es := store.New()
			ss := store.New()
			ctx := context.Background()

			if err := ss.Append(ctx, "snapshots:agg1", 1, benchSnapshotBlob(1, order{ID: "1", Status: "Pending", Total: 50})); err != nil {
				b.Fatal(err)
			}

			populateEvents(b, es, 2, tc.deltas)

			r := newTestReader(es, ss)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := r.Get(ctx, "agg1"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkGet_ColdPath(b *testing.B) {
	for _, tc := range []struct {
		name   string
		events int
	}{
		{"events=1", 1},
		{"events=10", 10},
		{"events=1_000_000", 1_000_000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			es := store.New()
			ctx := context.Background()
			populateEvents(b, es, 1, tc.events)
			r := newTestReader(es, store.New())
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := r.Get(ctx, "agg1"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
