package writer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	"github.com/char2cs/asynx/internal/store"
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

func populateEvents(b *testing.B, es *store.Memory, n int) {
	b.Helper()
	patch := json.RawMessage(`[{"op":"replace","path":"/Status","value":"x"}]`)
	ctx := context.Background()
	for i := 1; i <= n; i++ {
		if err := es.Append(ctx, "events:agg1", int64(i), benchEventBlob(int64(i), patch)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWrite(b *testing.B) {
	for _, tc := range []struct {
		name           string
		shouldSnapshot bool
	}{
		{"no_snapshot", false},
		{"with_snapshot", true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			var es *store.Memory
			var ss *store.Memory
			var w *Writer[order]
			prevState := order{Status: "Pending", Total: 50}
			nextState := order{Status: "Shipped", Total: 50}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				b.StopTimer()
				es = store.New()
				ss = store.New()
				w = newTestWriter(es, ss)
				b.StartTimer()
				if _, err := w.Write(ctx, "agg1", "Updated", prevState, nextState, tc.shouldSnapshot); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkNextVersion(b *testing.B) {
	for _, tc := range []struct {
		name   string
		events int
	}{
		{"no_history", 0},
		{"deltas=10_000", 10_000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			es := store.New()
			if tc.events > 0 {
				populateEvents(b, es, tc.events)
			}
			w := newTestWriter(es, store.New())
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := w.nextVersion(ctx, "agg1"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
