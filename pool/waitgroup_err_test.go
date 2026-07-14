package pool

import (
	"sort"
	"testing"
	"time"

	"github.com/vytharion/errgroup-vs-waitgroup-error-handling/jobs"
)

func TestRunAllWaitGroupErrCollectsEverySuccessAndNilError(t *testing.T) {
	batch := []jobs.Job{
		{ID: 1},
		{ID: 2},
		{ID: 3},
		{ID: 4},
	}

	results, err := RunAllWaitGroupErr(batch)
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

// The single-failure batch pins the whole point of this step: when exactly one
// goroutine returns a non-nil error, the caller finally sees it instead of a
// zero-value Result quietly stuffed into the slice.
func TestRunAllWaitGroupErrSurfacesTheFailure(t *testing.T) {
	batch := []jobs.Job{
		{ID: 10},
		{ID: 11, ShouldFail: true},
		{ID: 12},
	}

	results, err := RunAllWaitGroupErr(batch)
	if err == nil {
		t.Fatal("expected error from failing job, got nil")
	}
	if err.Error() != "job 11 failed" {
		t.Errorf("unexpected error message: %q", err.Error())
	}
	if len(results) != 2 {
		t.Fatalf("want 2 successful results alongside the error, got %d", len(results))
	}
	for _, r := range results {
		if r.JobID == 0 && r.Value == "" {
			t.Errorf("failing job leaked a zero-value Result into results slice: %+v", results)
		}
	}
}

// With multiple failures the contract is only "some error surfaces" — we
// cannot promise a specific one because goroutine scheduling picks the winner.
// This test locks that weaker guarantee so step 4's errgroup swap does not
// silently regress to returning nil.
func TestRunAllWaitGroupErrReturnsSomeErrorWhenSeveralFail(t *testing.T) {
	batch := []jobs.Job{
		{ID: 20, ShouldFail: true},
		{ID: 21, ShouldFail: true},
		{ID: 22},
	}

	results, err := RunAllWaitGroupErr(batch)
	if err == nil {
		t.Fatal("expected error when multiple jobs fail, got nil")
	}
	msg := err.Error()
	if msg != "job 20 failed" && msg != "job 21 failed" {
		t.Errorf("returned error came from neither failing job: %q", msg)
	}
	if len(results) != 1 {
		t.Fatalf("want 1 successful result, got %d", len(results))
	}
}

// Siblings are NOT cancelled when one goroutine errors — wg.Wait blocks until
// every worker finishes, so total wall time is dominated by the slowest job.
// This is the concrete cost that motivates the errgroup + context.Context
// upgrade in step 4.
func TestRunAllWaitGroupErrDoesNotCancelSiblings(t *testing.T) {
	const perJob = 30 * time.Millisecond
	batch := []jobs.Job{
		{ID: 1, Work: perJob},
		{ID: 2, ShouldFail: true},
		{ID: 3, Work: perJob},
		{ID: 4, Work: perJob},
		{ID: 5, Work: perJob},
	}

	start := time.Now()
	results, err := RunAllWaitGroupErr(batch)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if len(results) != 4 {
		t.Errorf("want 4 successful results (siblings kept running), got %d", len(results))
	}
	if elapsed < perJob {
		t.Errorf("call returned before slow siblings finished: %v", elapsed)
	}
	if elapsed >= 100*time.Millisecond {
		t.Errorf("pool ran too slowly to be concurrent: %v", elapsed)
	}
}
