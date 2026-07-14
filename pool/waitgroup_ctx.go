package pool

import (
	"context"
	"errors"
	"sync"

	"github.com/vytharion/errgroup-vs-waitgroup-error-handling/jobs"
)

// RunAllWaitGroupCtx keeps the sync.WaitGroup + buffered channel plumbing from
// RunAllWaitGroupErr but threads a cancel context through every goroutine. The
// first worker that returns a non-nil error calls cancel(), which flips
// ctx.Done for every sibling still sleeping inside jobs.DoCtx and lets them
// bail out immediately instead of burning their full Work budget.
//
// The returned error is the first *job* error observed. Context cancellation
// errors triggered by the sibling short-circuit are filtered out during the
// drain — they are a side effect of the fix, not a failure the caller cares
// about. If the caller-supplied ctx expires before any job fails, that
// deadline error is what surfaces.
func RunAllWaitGroupCtx(ctx context.Context, js []jobs.Job) ([]jobs.Result, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results = make([]jobs.Result, 0, len(js))
		errCh   = make(chan error, len(js))
	)
	for _, j := range js {
		wg.Add(1)
		go func(j jobs.Job) {
			defer wg.Done()
			r, err := jobs.DoCtx(ctx, j)
			if err != nil {
				errCh <- err
				cancel()
				return
			}
			mu.Lock()
			results = append(results, r)
			mu.Unlock()
		}(j)
	}
	wg.Wait()
	close(errCh)
	return results, firstJobError(ctx, errCh)
}

// firstJobError prefers a real job error over context cancellation noise.
// Siblings that bailed out because cancel() fired will surface context.Canceled;
// treating that as "the answer" would hide the actual failure. When every
// error is context noise, fall back to the caller-visible ctx error so an
// externally-cancelled call still reports something.
func firstJobError(ctx context.Context, errCh <-chan error) error {
	var jobErr, ctxErr error
	for e := range errCh {
		if isContextErr(e) {
			if ctxErr == nil {
				ctxErr = e
			}
			continue
		}
		if jobErr == nil {
			jobErr = e
		}
	}
	if jobErr != nil {
		return jobErr
	}
	if ctxErr != nil {
		return ctxErr
	}
	return ctx.Err()
}

func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
