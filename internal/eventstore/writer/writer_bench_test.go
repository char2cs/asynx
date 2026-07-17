package writer

import (
	"context"
	"testing"

	"github.com/char2cs/asynx/store"
)

func BenchmarkWrite(b *testing.B) {
	for _, tc := range []struct {
		name           string
		shouldSnapshot bool
	}{
		{"no_snapshot", false},
		{"with_snapshot", true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			var es store.Memory
			var ss store.SnapshotMemory
			var w *Writer[order]
			prevState := order{Status: "Pending", Total: 50}
			nextState := order{Status: "Shipped", Total: 50}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				b.StopTimer()
				es = store.New()
				ss = store.NewSnapshots()
				w = newTestWriter(es, ss)
				b.StartTimer()
				if _, err := w.Write(ctx, "agg1", "Updated", 0, prevState, nextState, tc.shouldSnapshot); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
