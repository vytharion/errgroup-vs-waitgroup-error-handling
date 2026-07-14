package pool

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/vytharion/errgroup-vs-waitgroup-error-handling/jobs"
)

func TestRunAllErrGroupCollectsEverySuccessAndNilError(t *testing.T) {
	batch := []jobs.Job{
		{ID: 1},
		{ID: 2},
		{ID: 3},
		{ID: 4},
	}

	results, err := RunAllErrGroup(context.Background(), batch)
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

// errgroup should behave like step 4's firstJobError filter without any of
// its bookkeeping: the real job error wins, the context.Canceled echoes
// from preempted siblings never surface.
func TestRunAllErrGroupSurfacesRealJobErrorNotContextNoise(t *testing.T) {
	batch := []jobs.Job{
		{ID: 10, Work: 50 * time.Millisecond},
		{ID: 11, ShouldFail: true},
		{ID: 12, Work: 50 * time.Millisecond},
	}

	_, err := RunAllErrGroup(context.Background(), batch)
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

// Same batch shape as step 4's sibling-cancel test — same assertion too,
// because the errgroup rewrite preserves the "return as fast as the fastest
// failure" invariant that context cancellation bought us in the previous step.
func TestRunAllErrGroupCancelsSiblingsOnFirstFailure(t *testing.T) {
	const perJob = 100 * time.Millisecond
	batch := []jobs.Job{
		{ID: 1, Work: perJob},
		{ID: 2, ShouldFail: true},
		{ID: 3, Work: perJob},
		{ID: 4, Work: perJob},
		{ID: 5, Work: perJob},
	}

	start := time.Now()
	_, err := RunAllErrGroup(context.Background(), batch)
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

// The caller-supplied ctx must still propagate. errgroup.WithContext wraps
// the parent, so a WithTimeout on the outside cancels every worker inside
// and the returned error is a context error, not a lie about success.
func TestRunAllErrGroupHonorsCallerCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	batch := []jobs.Job{
		{ID: 1, Work: 500 * time.Millisecond},
		{ID: 2, Work: 500 * time.Millisecond},
		{ID: 3, Work: 500 * time.Millisecond},
	}

	start := time.Now()
	results, err := RunAllErrGroup(ctx, batch)
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

// Empty batches must stay a (nil, nil) no-op. errgroup.Wait on a Group that
// never had Go called returns nil, so this falls out for free — but leaving
// it uncovered would make a future refactor easy to break silently.
func TestRunAllErrGroupHandlesEmptyBatch(t *testing.T) {
	results, err := RunAllErrGroup(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error on empty batch: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("empty batch should return zero results, got %d", len(results))
	}
}
