package pool

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/vytharion/errgroup-vs-waitgroup-error-handling/jobs"
)

// RunAllErrGroup is the errgroup rewrite of RunAllWaitGroupCtx. The four
// pieces of hand-rolled plumbing from step 4 — sync.WaitGroup, buffered
// errCh, sync.Mutex around results, and the firstJobError drain — all
// collapse into a single errgroup.WithContext call.
//
// errgroup captures the first non-nil error via sync.Once, cancels the
// shared context so siblings still inside jobs.DoCtx wake up immediately,
// and returns that first error from Wait. Every worker writes to its own
// slot in a preallocated slice, which sheds the mutex without introducing
// a race. On error we return a nil slice: partial completions during a
// failing batch are meaningless to the caller and matching step 4's
// len(results) == 0 contract on cancellation is easier if we surface
// nothing at all rather than a slice of zero-value Results.
func RunAllErrGroup(ctx context.Context, js []jobs.Job) ([]jobs.Result, error) {
	g, ctx := errgroup.WithContext(ctx)

	results := make([]jobs.Result, len(js))
	for i, j := range js {
		g.Go(func() error {
			r, err := jobs.DoCtx(ctx, j)
			if err != nil {
				return err
			}
			results[i] = r
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}
