package pool

import (
	"context"
	"strconv"
	"testing"

	"github.com/vytharion/errgroup-vs-waitgroup-error-handling/jobs"
)

// The benchmarks intentionally use Work: 0 jobs. Real HTTP-bound batches spend
// the overwhelming majority of their wall time inside jobs.DoCtx, which would
// swamp any pool-plumbing delta and make the errgroup-vs-hand-rolled comparison
// invisible. Stripping the sleep exposes the raw scheduling + synchronisation
// cost that each variant carries, which is the number the decision guide cares
// about.
func makeBenchBatch(n int) []jobs.Job {
	batch := make([]jobs.Job, n)
	for i := range batch {
		batch[i] = jobs.Job{ID: i}
	}
	return batch
}

// Unbounded comparison: RunAllErrGroup vs. RunAllWaitGroupCtx over 100 jobs.
// Both fan out one goroutine per job. errgroup carries the sync.Once + shared
// context; the hand-rolled version carries a WaitGroup, a mutex, a buffered
// errCh, and a firstJobError drain. Same happy-path work, different overhead.
func BenchmarkPoolsUnbounded(b *testing.B) {
	batch := makeBenchBatch(100)
	ctx := context.Background()
	variants := []struct {
		name string
		run  func() error
	}{
		{"ErrGroup", func() error { _, err := RunAllErrGroup(ctx, batch); return err }},
		{"WaitGroupCtx", func() error { _, err := RunAllWaitGroupCtx(ctx, batch); return err }},
	}
	for _, v := range variants {
		b.Run(v.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := v.run(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Bounded comparison at cap=8 over 100 jobs — the shape most real HTTP batches
// use. errgroup.SetLimit's internal token channel vs. the hand-rolled sem
// chan + acquireToken helper: same throttle policy, wildly different code
// surface, small but measurable overhead delta.
func BenchmarkPoolsBounded(b *testing.B) {
	batch := makeBenchBatch(100)
	ctx := context.Background()
	const limit = 8
	variants := []struct {
		name string
		run  func() error
	}{
		{"ErrGroupLimit", func() error { _, err := RunAllErrGroupLimit(ctx, batch, limit); return err }},
		{"WaitGroupSemaphore", func() error { _, err := RunAllWaitGroupSemaphore(ctx, batch, limit); return err }},
	}
	for _, v := range variants {
		b.Run(v.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := v.run(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Batch-size sweep for the two bounded variants. Amortising per-launch
// overhead across a growing batch shows whether one implementation degrades
// non-linearly — it doesn't, and the point of the article is to prove that
// with numbers rather than assertion.
func BenchmarkPoolsBoundedBySize(b *testing.B) {
	ctx := context.Background()
	const limit = 8
	sizes := []int{10, 100, 1000}
	for _, size := range sizes {
		batch := makeBenchBatch(size)
		label := "_N" + strconv.Itoa(size)
		b.Run("ErrGroupLimit"+label, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := RunAllErrGroupLimit(ctx, batch, limit); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("WaitGroupSemaphore"+label, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := RunAllWaitGroupSemaphore(ctx, batch, limit); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
