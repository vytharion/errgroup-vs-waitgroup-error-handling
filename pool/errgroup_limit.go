package pool

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/vytharion/errgroup-vs-waitgroup-error-handling/jobs"
)

// RunAllErrGroupLimit caps in-flight workers at `limit` using errgroup.SetLimit.
// That single method call is the entire concurrency-bounding mechanism — the
// semaphore rewrite in semaphore.go shows what it replaces: a buffered channel
// acquired before each launch, released in a deferred receive, and defended
// with an explicit ctx.Done branch so a caller-cancelled batch does not spawn
// goroutines it will immediately have to unwind.
//
// A non-positive limit means "no cap". errgroup's default is unbounded, so we
// simply skip SetLimit — passing 0 would create a zero-capacity channel and
// the first Go call would deadlock, which is a subtle enough footgun that the
// clamp belongs at the boundary.
//
// Everything else — first-error-wins, sibling cancellation via the shared ctx,
// return (nil, err) on failure — is inherited from RunAllErrGroup because the
// underlying Group object is the same. SetLimit only changes scheduling, not
// error semantics.
func RunAllErrGroupLimit(ctx context.Context, js []jobs.Job, limit int) ([]jobs.Result, error) {
	g, ctx := errgroup.WithContext(ctx)
	if limit > 0 {
		g.SetLimit(limit)
	}

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
