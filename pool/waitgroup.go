package pool

import (
	"sync"

	"github.com/vytharion/errgroup-vs-waitgroup-error-handling/jobs"
)

// RunAllWaitGroup fans out one goroutine per job, waits for the whole batch
// to finish, and returns whatever Results the workers produced.
//
// Errors from jobs.Do are intentionally discarded: sync.WaitGroup exposes no
// channel for them, and inventing one here would skip past the failure mode
// the next step is meant to make visible.
func RunAllWaitGroup(js []jobs.Job) []jobs.Result {
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results = make([]jobs.Result, 0, len(js))
	)
	for _, j := range js {
		wg.Add(1)
		go func(j jobs.Job) {
			defer wg.Done()
			r, _ := jobs.Do(j)
			mu.Lock()
			results = append(results, r)
			mu.Unlock()
		}(j)
	}
	wg.Wait()
	return results
}
