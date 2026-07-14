package pool

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/vytharion/errgroup-vs-waitgroup-error-handling/jobs"
)

func TestRunAllWaitGroupSemaphoreCollectsEverySuccessAndNilError(t *testing.T) {
	batch := []jobs.Job{
		{ID: 1},
		{ID: 2},
		{ID: 3},
		{ID: 4},
		{ID: 5},
		{ID: 6},
	}

	results, err := RunAllWaitGroupSemaphore(context.Background(), batch, 2)
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

// The semaphore rewrite must match errgroup.SetLimit's throttling behaviour to
// be a fair comparison in the article. Same batch shape, same expected floor.
func TestRunAllWaitGroupSemaphoreEnforcesConcurrencyCap(t *testing.T) {
	const perJob = 50 * time.Millisecond
	const limit = 2
	const total = 8
	batch := make([]jobs.Job, total)
	for i := range batch {
		batch[i] = jobs.Job{ID: i, Work: perJob}
	}

	start := time.Now()
	results, err := RunAllWaitGroupSemaphore(context.Background(), batch, limit)
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

// The clamp in RunAllWaitGroupSemaphore treats limit<=0 and limit>len(js) as
// "unbounded". Both must let the whole batch run in parallel.
func TestRunAllWaitGroupSemaphoreNonPositiveMeansUnbounded(t *testing.T) {
	const perJob = 100 * time.Millisecond
	batch := make([]jobs.Job, 5)
	for i := range batch {
		batch[i] = jobs.Job{ID: i, Work: perJob}
	}

	for _, limit := range []int{0, -1, 100} {
		start := time.Now()
		results, err := RunAllWaitGroupSemaphore(context.Background(), batch, limit)
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

// firstJobError must still prefer the real job error over the context.Canceled
// noise from the internal cancel() — otherwise the semaphore rewrite would
// regress the invariant we locked down in step 4.
func TestRunAllWaitGroupSemaphoreSurfacesRealJobErrorNotContextNoise(t *testing.T) {
	batch := []jobs.Job{
		{ID: 10, Work: 50 * time.Millisecond},
		{ID: 11, ShouldFail: true},
		{ID: 12, Work: 50 * time.Millisecond},
	}

	_, err := RunAllWaitGroupSemaphore(context.Background(), batch, 3)
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

// Sibling cancel via the internal cancel(): as soon as one goroutine reports a
// job error, the shared ctx flips and both the in-flight slow siblings and the
// acquire loop bail out. Elapsed time should be well under perJob.
func TestRunAllWaitGroupSemaphoreCancelsSiblingsOnFirstFailure(t *testing.T) {
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
	_, err := RunAllWaitGroupSemaphore(context.Background(), batch, limit)
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

// acquireToken's ctx.Err() precheck exists for exactly this test: a caller ctx
// that expires while workers are mid-flight (or before any launch) must be
// surfaced as a context error, not silently swallowed.
func TestRunAllWaitGroupSemaphoreHonorsCallerCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	batch := []jobs.Job{
		{ID: 1, Work: 500 * time.Millisecond},
		{ID: 2, Work: 500 * time.Millisecond},
		{ID: 3, Work: 500 * time.Millisecond},
	}

	start := time.Now()
	_, err := RunAllWaitGroupSemaphore(ctx, batch, 2)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected context error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("expected context error, got %v", err)
	}
	if elapsed >= 200*time.Millisecond {
		t.Errorf("caller cancel did not preempt workers: %v", elapsed)
	}
}

func TestRunAllWaitGroupSemaphoreHandlesEmptyBatch(t *testing.T) {
	results, err := RunAllWaitGroupSemaphore(context.Background(), nil, 4)
	if err != nil {
		t.Fatalf("unexpected error on empty batch: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("empty batch should return zero results, got %d", len(results))
	}
}

// Cross-check: the two implementations must produce equivalent results for
// identical inputs. This is the article's whole thesis — SetLimit and the
// semaphore idiom are behaviourally interchangeable; only one is readable.
func TestErrGroupLimitAndSemaphoreAgreeOnResults(t *testing.T) {
	batch := make([]jobs.Job, 12)
	for i := range batch {
		batch[i] = jobs.Job{ID: i}
	}

	egResults, err := RunAllErrGroupLimit(context.Background(), batch, 3)
	if err != nil {
		t.Fatalf("errgroup limit error: %v", err)
	}
	semResults, err := RunAllWaitGroupSemaphore(context.Background(), batch, 3)
	if err != nil {
		t.Fatalf("semaphore error: %v", err)
	}

	egIDs := collectIDs(egResults)
	semIDs := collectIDs(semResults)
	if len(egIDs) != len(semIDs) {
		t.Fatalf("result count mismatch: errgroup=%d semaphore=%d", len(egIDs), len(semIDs))
	}
	for i := range egIDs {
		if egIDs[i] != semIDs[i] {
			t.Errorf("id mismatch at %d: errgroup=%d semaphore=%d", i, egIDs[i], semIDs[i])
		}
	}
}

func collectIDs(rs []jobs.Result) []int {
	ids := make([]int, 0, len(rs))
	for _, r := range rs {
		ids = append(ids, r.JobID)
	}
	sort.Ints(ids)
	return ids
}
