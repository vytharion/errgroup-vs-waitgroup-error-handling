package jobs

import (
	"testing"
	"time"
)

func TestDoSucceeds(t *testing.T) {
	got, err := Do(Job{ID: 7})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.JobID != 7 {
		t.Errorf("want JobID=7, got %d", got.JobID)
	}
	if got.Value != "ok-7" {
		t.Errorf("want Value=ok-7, got %q", got.Value)
	}
}

func TestDoFailsDeterministically(t *testing.T) {
	_, err := Do(Job{ID: 42, ShouldFail: true})
	if err == nil {
		t.Fatal("expected error when ShouldFail is set, got nil")
	}
	if err.Error() != "job 42 failed" {
		t.Errorf("unexpected error message: %q", err.Error())
	}
}

func TestDoRespectsWorkDuration(t *testing.T) {
	start := time.Now()
	_, err := Do(Job{ID: 1, Work: 20 * time.Millisecond})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed < 20*time.Millisecond {
		t.Errorf("Do returned too fast: %v", elapsed)
	}
}
