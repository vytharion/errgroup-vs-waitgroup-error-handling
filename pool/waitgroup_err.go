package pool

import (
	"sync"

	"github.com/vytharion/errgroup-vs-waitgroup-error-handling/jobs"
)

// RunAllWaitGroupErr keeps the sync.WaitGroup fan-out from RunAllWaitGroup but
// threads a buffered error channel alongside it so the caller finally learns
// that at least one goroutine returned a non-nil error.
//
// The channel is sized to len(js) so every failing goroutine can send without
// blocking — otherwise a slow sender would deadlock behind wg.Wait. Successful
// results are still appended to the shared slice under mu; only the first
// error observed on drain is surfaced. Siblings are NOT cancelled — that is
// the deliberate weakness this step exposes and step 4 fixes with errgroup.
func RunAllWaitGroupErr(js []jobs.Job) ([]jobs.Result, error) {
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
			r, err := jobs.Do(j)
			if err != nil {
				errCh <- err
				return
			}
			mu.Lock()
			results = append(results, r)
			mu.Unlock()
		}(j)
	}
	wg.Wait()
	close(errCh)
	return results, firstError(errCh)
}

func firstError(errCh <-chan error) error {
	var first error
	for e := range errCh {
		if first == nil {
			first = e
		}
	}
	return first
}
