package pool

// Recommendation names the pool shape that fits a given set of Requirements.
// The four values map 1-to-1 onto the four implementations shipped in this
// package: RunAllErrGroup, RunAllErrGroupLimit, RunAllWaitGroupCtx,
// RunAllWaitGroupSemaphore. Any other combination is a design smell.
type Recommendation string

const (
	UseErrGroup             Recommendation = "errgroup.Group"
	UseErrGroupWithLimit    Recommendation = "errgroup.Group + SetLimit"
	UseWaitGroupWithChannel Recommendation = "sync.WaitGroup + buffered chan error"
	UseWaitGroupSemaphore   Recommendation = "sync.WaitGroup + buffered chan semaphore"
)

// Requirements captures the four axes that decide which pool shape a caller
// should reach for. The fields are all bools because in practice each axis
// is either a hard requirement or a non-issue — "sort of need all errors"
// never comes up in a real design review.
type Requirements struct {
	// Bounded means the caller must cap the number of in-flight workers.
	Bounded bool
	// CollectAllErrors means the caller wants every failure surfaced, not
	// just the first one. errgroup's sync.Once semantics rule it out.
	CollectAllErrors bool
	// PartialResultsOnFailure means a partial success slice must be returned
	// alongside the error. errgroup's Wait discards partials by contract.
	PartialResultsOnFailure bool
	// CustomThrottling means the concurrency policy is anything other than
	// "at most N at a time" — rate limits, priority queues, per-key mutexes.
	// SetLimit runs out of expressiveness immediately.
	CustomThrottling bool
}

// Recommend maps a Requirements value to the pool shape that satisfies it
// with the smallest reviewer checklist. The order of checks encodes the
// decision-guide's priority: CustomThrottling forces the semaphore because
// it is the only shape whose token bucket can be swapped for a richer
// policy; then error-model constraints rule errgroup out; then bounded-vs-
// unbounded picks between the remaining candidates.
func Recommend(r Requirements) Recommendation {
	if r.CustomThrottling {
		return UseWaitGroupSemaphore
	}
	if r.CollectAllErrors || r.PartialResultsOnFailure {
		if r.Bounded {
			return UseWaitGroupSemaphore
		}
		return UseWaitGroupWithChannel
	}
	if r.Bounded {
		return UseErrGroupWithLimit
	}
	return UseErrGroup
}
