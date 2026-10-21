# Reliable Job Runner design

## Problem

Applications push slow or unreliable work into the background: sending an email, generating a
report, calling a flaky API. That work must survive things going wrong:

- the task fails temporarily (a timeout, a rate limit) and should be retried, but not forever;
- the task fails permanently (bad input) and should stop retrying and record why;
- the worker crashes or freezes halfway through, and the job must not be lost;
- the client submits the same request twice (a double click, a retried HTTP call), and the work
  must not happen twice.

This project builds a single-machine job queue that handles each case explicitly, with tests
that simulate every one of them.

## Scope of the MVP

- Submit a job with a kind, a JSON payload, and an optional idempotency key; check its status.
- Store jobs in SQLite so they survive a restart.
- One worker claims a job with a time-limited **lease**, runs it, and records the result.
- Bounded retries with exponential backoff; jobs that run out of attempts are marked failed with
  their last error.
- A job whose lease expires (worker crashed or froze) is recovered and retried.
- A small HTTP API for submitting jobs and reading their status.
- A harmless demo job: summarizing a local CSV file.

Out of scope for the MVP: multiple machines, priorities, cancellation, scheduling at a set time,
and a dashboard.

## Delivery guarantee: at least once, not exactly once

Consider a worker that finishes a job and then crashes **before** it records the job as done. To
the system, that looks exactly like a worker that crashed before doing anything. The only safe
response is to run the job again, so some work can run twice. No queue can tell those two cases
apart without the job's cooperation, so "exactly once" can't be guaranteed in general.

This runner therefore promises **at-least-once execution**, and makes duplicates harmless in two
ways:

1. **Idempotency keys on submission.** Submitting with a key that already exists returns the
   existing job instead of creating a second one.
2. **Idempotent job handlers.** Each handler is written so running it twice has the same effect
   as running it once. The demo writes its output with an atomic rename, so a second run simply
   replaces the file with an identical one.

## Job lifecycle

```
 submit ──► queued ──── claim (when run_after ≤ now) ────► running ──── success ────► succeeded
              ▲                                              │
              │  retryable failure or lease expired,         │  permanent failure, or
              └───────── attempts left: wait (backoff) ──────┤  no attempts left
                                                             └──────────────────────► failed
```

`succeeded` and `failed` are terminal: nothing moves a job out of them.

| State | Meaning |
|---|---|
| `queued` | Waiting to run, not before `run_after`. |
| `running` | Claimed by a worker that holds a lease until `lease_expires_at`. |
| `succeeded` | Finished; `result` holds the output. Terminal. |
| `failed` | Gave up; `last_error` says why. Terminal. |

## Leases and fencing

A worker claims a job for a fixed time (its lease) and may extend it while working. If the
lease runs out, the job is considered abandoned and goes back to the queue.

That creates a danger: a worker that was only **slow**, not dead, may come back after its lease
expired and the job was handed to another worker. If the slow worker could still record "done",
it might overwrite the newer attempt's result.

So each claim gets a **fencing token**: the attempt number. Completing, failing, or extending a
job requires the token of the current attempt. A worker holding an old token is refused, so a
stale worker can never change a job it no longer owns.

## Retries and backoff

After a retryable failure on attempt *n*, the job waits before it can be claimed again. The wait
uses exponential backoff with "full jitter": a random delay between 0 and
`min(cap, base × 2^(n−1))`. Exponential growth stops a struggling dependency from being hammered,
and the randomness keeps many failed jobs from all retrying at the same moment.

The random source is passed in, so tests can make it deterministic.

## Time

Times are integers in milliseconds, and the current time is always passed in, never read inside
the logic. Tests can therefore jump a lease past its expiry instantly instead of sleeping, and
every transition is deterministic.

## Invariants (what the tests must prove)

- A job only moves along the arrows in the lifecycle above; anything else is rejected.
- `succeeded` and `failed` never change again.
- `attempts` never exceeds `max_attempts`, and every claim increases it by exactly one.
- A `queued` job always has an attempt left. Otherwise it could never run and never fail:
  it would wait forever. (Found when a deliberately broken retry limit got past the first
  version of these invariants.)
- A `running` job always has a lease owner and an expiry; no other state has either.
- A request with a stale fencing token never changes a job.
- A `succeeded` job has a result; a `failed` job has an error.
- Backoff delays are always between 0 and the cap.

## Milestones

1. Design and the job state machine as pure functions, with stateful property tests.
2. SQLite storage with safe claiming: only one worker can claim a job.
3. Worker loop: run handlers, record results, retry with backoff, recover expired leases.
4. Idempotent submission and an HTTP API.
5. Crash and failure demonstrations, measured behavior.
6. README, architecture notes, and limitations.
