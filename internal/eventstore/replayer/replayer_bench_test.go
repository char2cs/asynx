package replayer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	"github.com/char2cs/asynx/internal/store"
	asynxmd "github.com/char2cs/asynx/models"
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

func BenchmarkHydrate(b *testing.B) {
	for _, tc := range []struct {
		name   string
		events int
	}{
		{"events=1", 1},
		{"events=10", 10},
		{"events=10_000", 10_000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			patch := json.RawMessage(`[{"op":"replace","path":"/Status","value":"x"}]`)
			now := time.Now().UTC()
			events := make([]esmodels.InternalEvent, tc.events)
			for i := range events {
				events[i] = esmodels.InternalEvent{
					ID:            "bench",
					EventName:     "Updated",
					Version:       int64(i + 1),
					SchemaVersion: 1,
					OccurredAt:    now,
					Patches:       patch,
				}
			}
			r := newTestReplayer(store.New(), nil, 1)
			seed := order{}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := r.Hydrate(ctx, "agg1", seed, events); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkReplay(b *testing.B) {
	for _, tc := range []struct {
		name   string
		events int
	}{
		{"events=1", 1},
		{"events=10", 10},
		{"events=10_000", 10_000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			es := store.New()
			populateEvents(b, es, tc.events)
			r := newTestReplayer(es, nil, 1)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := r.Replay(ctx, "agg1", 1, 0, func(ctx context.Context, e asynxmd.Event[order]) {}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
