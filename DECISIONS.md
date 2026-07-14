# Pool decision guide

This package ships four goroutine-pool shapes because no single one covers
every real-world workload. Use the table below to pick — or call
`pool.Recommend(pool.Requirements{...})` if you'd rather encode the
decision as data.

## The four shapes

| Function | Bounded? | Error model | Partial results on failure? |
| --- | --- | --- | --- |
| `RunAllErrGroup` | no | first-error-wins | no (returns `nil`) |
| `RunAllErrGroupLimit` | yes (`SetLimit`) | first-error-wins | no (returns `nil`) |
| `RunAllWaitGroupCtx` | no | first-error-wins via `firstJobError` | no (returns `nil`) |
| `RunAllWaitGroupSemaphore` | yes (buffered `chan struct{}`) | first-error-wins via `firstJobError` | no (returns `nil`) |

`RunAllWaitGroupErr` and `RunAllWaitGroup` (steps 2 and 3) exist only as
teaching artefacts — do not reach for them in new code.

## When to pick which

1. **Default: `RunAllErrGroup` / `RunAllErrGroupLimit`.** If you need
   first-error-wins semantics and either "no cap" or "at most N", the
   errgroup variants own that space. They cost the least reviewer
   attention: sibling cancel, error preference, and the `SetLimit` token
   bucket are all inherited from the library.
2. **Need to collect every error, not just the first.** `errgroup.Group`
   captures errors via `sync.Once` — the second and third failures are
   dropped on the floor. Fall back to the buffered-`chan error` pattern
   in `RunAllWaitGroupCtx` / `RunAllWaitGroupSemaphore` and drain the
   channel yourself.
3. **Need partial results on failure.** `errgroup.Wait` discards the
   `results` slice by convention when it returns an error. The
   mutex-guarded `append` in the WaitGroup variants gives you a partial
   slice you can hand back to the caller alongside the error.
4. **Custom throttling policy.** Rate limits, priority queues, per-key
   locks, adaptive back-off — anything richer than "at most N in flight"
   outgrows `SetLimit`. Start from `RunAllWaitGroupSemaphore` and swap
   the token bucket for the policy you actually want.

## Benchmarks

Run the pool benchmarks with:

```bash
go test ./pool/ -bench=BenchmarkPools -benchmem -run=^$
```

The unbounded and bounded comparisons run the same 100-job batch through
both shapes with `Work: 0`, isolating the pool-plumbing overhead from
real per-job cost. On any realistic workload — HTTP calls, DB queries,
CPU-bound compute — the per-job time dominates the sub-microsecond
scheduling delta by three or four orders of magnitude, so the number
that actually matters is reviewer clarity, not nanoseconds.

The batch-size sweep (`BenchmarkPoolsBoundedBySize`) confirms both
bounded variants scale linearly with batch size. Neither has a hidden
cliff at `N=1000`.

## Rule of thumb

If you are writing the pool from scratch and none of "collect every
error", "return partial results on failure", or "custom throttling"
applies, reach for `errgroup.Group` and — if you need a cap —
`SetLimit`. Rewriting the same pool with a `WaitGroup`, a mutex, a
buffered `errCh`, and a semaphore is a lot of code to write, review,
and debug for behaviour that a single import gives you for free.
