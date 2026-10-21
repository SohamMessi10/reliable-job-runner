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

## Commands verified

| Where | Python | Command | Result |
|---|---|---|---|
| Claude's Linux workspace | 3.11, 3.13, 3.14 | `python -m pytest`, dev and ci profiles | 39 passed |

## Next

- Milestone 2: SQLite storage with safe claiming, so only one worker can claim a job.

## Concepts to review
- At-least-once vs exactly-once delivery, and why idempotency matters.
- Leases, and fencing tokens against stale workers.
- Exponential backoff with full jitter.
- Injecting time and randomness to make tests deterministic.
- Stateful property-based testing (Hypothesis `RuleBasedStateMachine`).
