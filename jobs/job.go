package jobs

import (
	"context"
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

// DoCtx is the context-aware sibling of Do. It sleeps on a select with
// ctx.Done so an external cancel can preempt the fake work — this is what
// lets a pool built on top of it short-circuit sibling goroutines the moment
// one worker reports an error.
func DoCtx(ctx context.Context, job Job) (Result, error) {
	if err := sleepCtx(ctx, job.Work); err != nil {
		return Result{}, err
	}
	if job.ShouldFail {
		return Result{}, fmt.Errorf("job %d failed", job.ID)
	}
	return Result{JobID: job.ID, Value: fmt.Sprintf("ok-%d", job.ID)}, nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
