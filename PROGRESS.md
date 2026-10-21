# Progress

## Completed

### Milestone 1: Design and the job state machine
- `docs/design.md`: problem, scope, why delivery is at least once (not exactly once), the job
  lifecycle, leases and fencing tokens, backoff, injected time, and the invariants.
- `jobs.py`: pure transitions `submit`, `claim`, `extend_lease`, `complete`, `fail`, and
  `expire_lease` on immutable `Job` values, plus `RetryPolicy.backoff` (exponential with full
  jitter and a cap). Time is integer milliseconds passed in; randomness is passed in.
- Fencing: completing, failing, or extending needs the current attempt number, and an expired
  lease can't be used even by its owner.
- Tests: example tests for each transition, and a Hypothesis stateful test where several
  workers claim, stall, retry, and report late with old tokens in random order. Every
  rejected request must leave the job unchanged.
- Measured what the stateful test reaches. Widening its time jumps raised runs with a stale
  token around from 23 to 154 and lease recoveries from 46 to 399.
- Mutation testing: 10 deliberate bugs all caught. Checking the stateful test alone exposed a
  missing invariant: a job could be requeued with no attempts left and wait forever. Added "a
  queued job always has an attempt left".

### Milestone 2: SQLite storage and safe claiming
- `JobStore.open(path, create=...)`: one `jobs` table in WAL mode with CHECK constraints that
  mirror the invariants, a UNIQUE idempotency key, and partial indexes for queued and running
  jobs.
- `submit` (idempotent by key; a reused key with a different request is refused), `claim_next`
  (oldest runnable first), `extend_lease`, `complete`, `fail`, `recover_expired`, `get`, `list`,
  `counts`. Each step is one `BEGIN IMMEDIATE` transaction around a pure transition, and every
  UPDATE checks that exactly one row matched what was read.
- Race test: 4 or 8 separate processes compete for 120 or 160 jobs; each job must be claimed
  exactly once, by several workers. The first version barely raced (one worker took 186 of 200
  jobs), so workers now signal "ready" before a shared start signal and do simulated work. The
  test passed 20 of 20 runs. With the locking removed, 10 jobs were claimed twice, and the test
  caught it in 5 of 5 runs.
- Mutation testing also showed two redundant settings: a busy-timeout pragma that repeated
  Python's default 5-second lock wait (now one named setting, proven necessary: with zero wait the
  race fails with "database is locked") and an early JSON check that the transaction already
  covered (removed).

## Commands verified

| Where | Python | Command | Result |
|---|---|---|---|
| Claude's Linux workspace | 3.11, 3.13, 3.14 | `python -m pytest`, dev and ci profiles | 39 passed (milestone 1) |
| GitHub Actions | 3.11–3.14, macOS 3.13 | `tests` workflow on `8a79625` | was still running when checked |
| Claude's Linux workspace | 3.11, 3.13, 3.14 | `python -m pytest`, dev and ci profiles | 77 passed (milestone 2) |

## Next

- Milestone 3: the worker loop: run handlers, record results, retry with backoff, recover expired leases.

## Concepts to review
- At-least-once vs exactly-once delivery, and why idempotency matters.
- Leases, and fencing tokens against stale workers.
- Exponential backoff with full jitter.
- Injecting time and randomness to make tests deterministic.
- Stateful property-based testing (Hypothesis `RuleBasedStateMachine`).
- Race conditions: why read-then-write needs one transaction; `BEGIN IMMEDIATE`; WAL mode.
- Optimistic checks (`WHERE ... AND state = ?`) as a backstop.
- Idempotency keys, and why reusing one for a different request is an error.
- Testing concurrency honestly: making sure the race actually happens.
