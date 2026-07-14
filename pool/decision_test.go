package pool

import "testing"

func TestRecommendDefaultsToErrGroup(t *testing.T) {
	got := Recommend(Requirements{})
	if got != UseErrGroup {
		t.Errorf("baseline: want %q, got %q", UseErrGroup, got)
	}
}

func TestRecommendBoundedFirstErrorWinsPicksSetLimit(t *testing.T) {
	got := Recommend(Requirements{Bounded: true})
	if got != UseErrGroupWithLimit {
		t.Errorf("bounded: want %q, got %q", UseErrGroupWithLimit, got)
	}
}

func TestRecommendCollectAllErrorsRulesOutErrGroup(t *testing.T) {
	got := Recommend(Requirements{CollectAllErrors: true})
	if got != UseWaitGroupWithChannel {
		t.Errorf("collect-all: want %q, got %q", UseWaitGroupWithChannel, got)
	}
}

func TestRecommendPartialResultsRuleOutErrGroup(t *testing.T) {
	got := Recommend(Requirements{PartialResultsOnFailure: true})
	if got != UseWaitGroupWithChannel {
		t.Errorf("partial-results: want %q, got %q", UseWaitGroupWithChannel, got)
	}
}

func TestRecommendCollectAllPlusBoundedPicksSemaphore(t *testing.T) {
	got := Recommend(Requirements{CollectAllErrors: true, Bounded: true})
	if got != UseWaitGroupSemaphore {
		t.Errorf("collect-all+bounded: want %q, got %q", UseWaitGroupSemaphore, got)
	}
}

func TestRecommendPartialPlusBoundedPicksSemaphore(t *testing.T) {
	got := Recommend(Requirements{PartialResultsOnFailure: true, Bounded: true})
	if got != UseWaitGroupSemaphore {
		t.Errorf("partial+bounded: want %q, got %q", UseWaitGroupSemaphore, got)
	}
}

func TestRecommendCustomThrottlingForcesSemaphore(t *testing.T) {
	got := Recommend(Requirements{CustomThrottling: true})
	if got != UseWaitGroupSemaphore {
		t.Errorf("custom-throttling: want %q, got %q", UseWaitGroupSemaphore, got)
	}
}

// Custom throttling on top of collect-all still picks the semaphore — the
// semaphore is a superset of both requirements, so no need to escalate to a
// hypothetical fifth variant.
func TestRecommendCustomThrottlingWithCollectAllStaysSemaphore(t *testing.T) {
	got := Recommend(Requirements{CustomThrottling: true, CollectAllErrors: true})
	if got != UseWaitGroupSemaphore {
		t.Errorf("custom+collect-all: want %q, got %q", UseWaitGroupSemaphore, got)
	}
}

// Table sweep so a new axis added to Requirements shows up here as a missing
// row and forces the author to think about it.
func TestRecommendMatrix(t *testing.T) {
	cases := []struct {
		name string
		req  Requirements
		want Recommendation
	}{
		{"none", Requirements{}, UseErrGroup},
		{"bounded", Requirements{Bounded: true}, UseErrGroupWithLimit},
		{"collect-all", Requirements{CollectAllErrors: true}, UseWaitGroupWithChannel},
		{"partial", Requirements{PartialResultsOnFailure: true}, UseWaitGroupWithChannel},
		{"custom-throttle", Requirements{CustomThrottling: true}, UseWaitGroupSemaphore},
		{"bounded+collect-all", Requirements{Bounded: true, CollectAllErrors: true}, UseWaitGroupSemaphore},
		{"bounded+partial", Requirements{Bounded: true, PartialResultsOnFailure: true}, UseWaitGroupSemaphore},
		{"bounded+custom", Requirements{Bounded: true, CustomThrottling: true}, UseWaitGroupSemaphore},
		{"partial+custom", Requirements{PartialResultsOnFailure: true, CustomThrottling: true}, UseWaitGroupSemaphore},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Recommend(tc.req)
			if got != tc.want {
				t.Errorf("Recommend(%+v) = %q, want %q", tc.req, got, tc.want)
			}
		})
	}
}
