package pool

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/vytharion/errgroup-vs-waitgroup-error-handling/jobs"
)

func TestRunAllWaitGroupCtxCollectsEverySuccessAndNilError(t *testing.T) {
	batch := []jobs.Job{
		{ID: 1},
		{ID: 2},
		{ID: 3},
		{ID: 4},
	}

	results, err := RunAllWaitGroupCtx(context.Background(), batch)
	if err != nil {
		t.Fatalf("unexpected error on all-success batch: %v", err)
	}
	if len(results) != len(batch) {
		t.Fatalf("want %d results, got %d", len(batch), len(results))
	}

	ids := make([]int, 0, len(results))
	for _, r := range results {
		ids = append(ids, r.JobID)
	}
	sort.Ints(ids)
	for i, want := range []int{1, 2, 3, 4} {
		if ids[i] != want {
			t.Errorf("missing JobID %d in results: got %v", want, ids)
		}
	}
}

// A single failure must still surface as a real job error even though the
// cancel() call inevitably produces context.Canceled noise from preempted
// siblings. firstJobError filters that noise out; this test locks that in.
func TestRunAllWaitGroupCtxSurfacesRealJobErrorNotContextNoise(t *testing.T) {
	batch := []jobs.Job{
		{ID: 10, Work: 50 * time.Millisecond},
		{ID: 11, ShouldFail: true},
		{ID: 12, Work: 50 * time.Millisecond},
	}

	_, err := RunAllWaitGroupCtx(context.Background(), batch)
	if err == nil {
		t.Fatal("expected error from failing job, got nil")
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("returned context.Canceled instead of the real job error: %v", err)
	}
	if err.Error() != "job 11 failed" {
		t.Errorf("unexpected error message: %q", err.Error())
	}
}

// The core step-4 promise: when one goroutine fails, siblings are cancelled
// and the whole call returns quickly instead of dragging on until the slowest
// worker's Work budget elapses. Compare this test's tolerance (< perJob) to
// step 3's equivalent (>= perJob) — same shape, inverted expectation.
func TestRunAllWaitGroupCtxCancelsSiblingsOnFirstFailure(t *testing.T) {
	const perJob = 100 * time.Millisecond
	batch := []jobs.Job{
		{ID: 1, Work: perJob},
		{ID: 2, ShouldFail: true},
		{ID: 3, Work: perJob},
		{ID: 4, Work: perJob},
		{ID: 5, Work: perJob},
	}

	start := time.Now()
	_, err := RunAllWaitGroupCtx(context.Background(), batch)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "job 2 failed" {
		t.Errorf("expected first job error to win, got %q", err.Error())
	}
	if elapsed >= perJob {
		t.Errorf("siblings were not cancelled — elapsed %v ≥ perJob %v", elapsed, perJob)
	}
}

// External cancellation must propagate: when the caller's ctx expires before
// any job fails, every worker bails out and the caller sees context.Canceled
// (or DeadlineExceeded), not a nil error.
func TestRunAllWaitGroupCtxHonorsCallerCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	batch := []jobs.Job{
		{ID: 1, Work: 500 * time.Millisecond},
		{ID: 2, Work: 500 * time.Millisecond},
		{ID: 3, Work: 500 * time.Millisecond},
	}

	start := time.Now()
	results, err := RunAllWaitGroupCtx(ctx, batch)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected context error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("expected context error, got %v", err)
	}
	if len(results) != 0 {
		t.Errorf("no jobs should have completed before cancel, got %d", len(results))
	}
	if elapsed >= 200*time.Millisecond {
		t.Errorf("caller cancel did not preempt workers: %v", elapsed)
	}
}

// An empty batch is a degenerate but easy-to-regress case: no goroutines fan
// out, no errors are ever sent, and the caller should still get (nil, nil).
func TestRunAllWaitGroupCtxHandlesEmptyBatch(t *testing.T) {
	results, err := RunAllWaitGroupCtx(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error on empty batch: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("empty batch should return zero results, got %d", len(results))
	}
}
