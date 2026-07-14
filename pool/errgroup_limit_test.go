package pool

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/vytharion/errgroup-vs-waitgroup-error-handling/jobs"
)

func TestRunAllErrGroupLimitCollectsEverySuccessAndNilError(t *testing.T) {
	batch := []jobs.Job{
		{ID: 1},
		{ID: 2},
		{ID: 3},
		{ID: 4},
		{ID: 5},
		{ID: 6},
	}

	results, err := RunAllErrGroupLimit(context.Background(), batch, 2)
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
	for i, want := range []int{1, 2, 3, 4, 5, 6} {
		if ids[i] != want {
			t.Errorf("missing JobID %d in results: got %v", want, ids)
		}
	}
}

// The core new invariant this step buys us: with 8 jobs of 50ms each and a cap
// of 2, wall-clock completion must be roughly (N/limit)*perJob = 200ms. If the
// cap silently no-oped we would see ~50ms (everyone in parallel). Anything
// meaningfully below the (N/limit)*perJob floor means SetLimit didn't stick.
func TestRunAllErrGroupLimitEnforcesConcurrencyCap(t *testing.T) {
	const perJob = 50 * time.Millisecond
	const limit = 2
	const total = 8
	batch := make([]jobs.Job, total)
	for i := range batch {
		batch[i] = jobs.Job{ID: i, Work: perJob}
	}

	start := time.Now()
	results, err := RunAllErrGroupLimit(context.Background(), batch, limit)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != total {
		t.Fatalf("want %d results, got %d", total, len(results))
	}
	want := time.Duration(total/limit) * perJob
	floor := want * 8 / 10
	if elapsed < floor {
		t.Errorf("limit %d not enforced: elapsed %v < floor %v (ideal %v)", limit, elapsed, floor, want)
	}
}

// A non-positive limit is the "no cap" contract. With Work=100ms across 5
// jobs and no cap, they all run in parallel and the whole batch finishes in
// well under 2*perJob. A limit greater than the batch size behaves the same.
func TestRunAllErrGroupLimitNonPositiveMeansUnbounded(t *testing.T) {
	const perJob = 100 * time.Millisecond
	batch := make([]jobs.Job, 5)
	for i := range batch {
		batch[i] = jobs.Job{ID: i, Work: perJob}
	}

	for _, limit := range []int{0, -1, 100} {
		start := time.Now()
		results, err := RunAllErrGroupLimit(context.Background(), batch, limit)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("limit=%d: unexpected error: %v", limit, err)
		}
		if len(results) != len(batch) {
			t.Fatalf("limit=%d: want %d results, got %d", limit, len(batch), len(results))
		}
		if elapsed >= 2*perJob {
			t.Errorf("limit=%d: expected parallel execution, elapsed=%v", limit, elapsed)
		}
	}
}

// Bounding concurrency must not weaken the first-error-wins guarantee inherited
// from step 5. Give the failing job a slot alongside the slow ones and it must
// still be its error — not context.Canceled — that surfaces.
func TestRunAllErrGroupLimitSurfacesRealJobErrorNotContextNoise(t *testing.T) {
	batch := []jobs.Job{
		{ID: 10, Work: 50 * time.Millisecond},
		{ID: 11, ShouldFail: true},
		{ID: 12, Work: 50 * time.Millisecond},
	}

	_, err := RunAllErrGroupLimit(context.Background(), batch, 3)
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

// Same shape as step 5's sibling-cancel test but with a limit that leaves room
// for both the failing job and at least one slow sibling. The cancellation
// invariant is unchanged — SetLimit does not interfere with WithContext.
func TestRunAllErrGroupLimitCancelsSiblingsOnFirstFailure(t *testing.T) {
	const perJob = 100 * time.Millisecond
	const limit = 3
	batch := []jobs.Job{
		{ID: 1, Work: perJob},
		{ID: 2, ShouldFail: true},
		{ID: 3, Work: perJob},
		{ID: 4, Work: perJob},
		{ID: 5, Work: perJob},
	}

	start := time.Now()
	_, err := RunAllErrGroupLimit(context.Background(), batch, limit)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "job 2 failed" {
		t.Errorf("expected first job error to win, got %q", err.Error())
	}
	if elapsed >= perJob {
		t.Errorf("siblings were not cancelled — elapsed %v >= perJob %v", elapsed, perJob)
	}
}

func TestRunAllErrGroupLimitHonorsCallerCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	batch := []jobs.Job{
		{ID: 1, Work: 500 * time.Millisecond},
		{ID: 2, Work: 500 * time.Millisecond},
		{ID: 3, Work: 500 * time.Millisecond},
	}

	start := time.Now()
	results, err := RunAllErrGroupLimit(ctx, batch, 2)
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

func TestRunAllErrGroupLimitHandlesEmptyBatch(t *testing.T) {
	results, err := RunAllErrGroupLimit(context.Background(), nil, 4)
	if err != nil {
		t.Fatalf("unexpected error on empty batch: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("empty batch should return zero results, got %d", len(results))
	}
}
