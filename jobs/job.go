package jobs

import (
	"fmt"
	"time"
)

type Job struct {
	ID         int
	ShouldFail bool
	Work       time.Duration
}

type Result struct {
	JobID int
	Value string
}

// Do simulates a unit of work. When ShouldFail is set it returns an error
// deterministically — that determinism is the whole point of the harness,
// because later steps need to reason about "what happens when goroutine N fails".
func Do(job Job) (Result, error) {
	if job.Work > 0 {
		time.Sleep(job.Work)
	}
	if job.ShouldFail {
		return Result{}, fmt.Errorf("job %d failed", job.ID)
	}
	return Result{JobID: job.ID, Value: fmt.Sprintf("ok-%d", job.ID)}, nil
}
