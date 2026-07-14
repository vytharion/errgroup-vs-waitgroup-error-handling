package pool

import (
	"sort"
	"testing"
	"time"

	"github.com/vytharion/errgroup-vs-waitgroup-error-handling/jobs"
)

func TestRunAllWaitGroupCollectsEverySuccess(t *testing.T) {
	batch := []jobs.Job{
		{ID: 1},
		{ID: 2},
		{ID: 3},
		{ID: 4},
	}

	results := RunAllWaitGroup(batch)

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

// A failing job returns a zero-value Result. The pool records that zero
// alongside successes and gives the caller no signal that anything went
// wrong — the whole point of the baseline this step establishes.
func TestRunAllWaitGroupSilentlyDropsErrors(t *testing.T) {
	batch := []jobs.Job{
		{ID: 10},
		{ID: 11, ShouldFail: true},
		{ID: 12},
	}

	results := RunAllWaitGroup(batch)

	if len(results) != 3 {
		t.Fatalf("want 3 results (successes + zero-value for failure), got %d", len(results))
	}

	var zeros, oks int
	for _, r := range results {
		if r.JobID == 0 && r.Value == "" {
			zeros++
			continue
		}
		oks++
	}
	if zeros != 1 {
		t.Errorf("want exactly 1 zero-value Result from the failing job, got %d", zeros)
	}
	if oks != 2 {
		t.Errorf("want 2 successful Results, got %d", oks)
	}
}

func TestRunAllWaitGroupRunsConcurrently(t *testing.T) {
	const perJob = 30 * time.Millisecond
	batch := []jobs.Job{
		{ID: 1, Work: perJob},
		{ID: 2, Work: perJob},
		{ID: 3, Work: perJob},
		{ID: 4, Work: perJob},
		{ID: 5, Work: perJob},
	}

	start := time.Now()
	results := RunAllWaitGroup(batch)
	elapsed := time.Since(start)

	if len(results) != len(batch) {
		t.Fatalf("want %d results, got %d", len(batch), len(results))
	}
	// Sequential execution would take 5 * 30ms = 150ms. A concurrent
	// fan-out should finish comfortably under half of that even on a
	// loaded machine.
	if elapsed >= 100*time.Millisecond {
		t.Errorf("RunAllWaitGroup ran too slowly to be concurrent: %v", elapsed)
	}
}
