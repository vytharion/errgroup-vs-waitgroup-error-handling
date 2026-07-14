package pool

import (
	"context"
	"sync"

	"github.com/vytharion/errgroup-vs-waitgroup-error-handling/jobs"
)

// RunAllWaitGroupSemaphore is the hand-rolled comparison against
// RunAllErrGroupLimit's single SetLimit call. A buffered channel of size
// `limit` acts as the token bucket: send before starting a job, receive after
// it finishes. This is the classical Go concurrency-cap idiom.
//
// A non-positive `limit` (or one larger than the batch) is clamped to len(js)
// so an empty channel with capacity 0 can never deadlock the acquire.
//
// The tricky parts, none of which errgroup.SetLimit makes the caller think
// about, are:
//
//  1. Acquire must be cancel-aware. If ctx is already done when we try to
//     send on `sem`, we must fail the acquire rather than launch a goroutine
//     that will just observe ctx.Err() a moment later.
//  2. Release must happen even on error, or the pool deadlocks on the next
//     acquire — hence the deferred `<-sem` inside the worker.
//  3. First-error-wins vs. context noise is still on us: siblings preempted
//     by the internal cancel() surface context.Canceled, which we filter out
//     via firstJobError so the real job error wins.
//
// Compare the line count and the number of things you have to keep straight
// against RunAllErrGroupLimit — that gap is the point of this file.
func RunAllWaitGroupSemaphore(ctx context.Context, js []jobs.Job, limit int) ([]jobs.Result, error) {
	if len(js) == 0 {
		return nil, nil
	}
	if limit <= 0 || limit > len(js) {
		limit = len(js)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results = make([]jobs.Result, 0, len(js))
		errCh   = make(chan error, len(js))
		sem     = make(chan struct{}, limit)
	)

	for _, j := range js {
		if !acquireToken(ctx, sem) {
			errCh <- ctx.Err()
			break
		}
		wg.Add(1)
		go func(j jobs.Job) {
			defer wg.Done()
			defer func() { <-sem }()
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
	if err := firstJobError(ctx, errCh); err != nil {
		return nil, err
	}
	return results, nil
}

// acquireToken deliberately checks ctx.Err() before entering the select. A
// bare select on {ctx.Done, sem<-} picks a ready case at random, so if both
// are ready we might send a token onto a doomed-ctx pool and spawn a goroutine
// that immediately unwinds. Checking first makes the fast-fail deterministic.
func acquireToken(ctx context.Context, sem chan struct{}) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	case sem <- struct{}{}:
		return true
	}
}
