"""The worker loop: recover abandoned jobs, claim one, run it, record what happened.

A handler is a function `handler(payload, ctx) -> result`. It reports failures by
raising:

- `Retryable(...)`: temporary, try again later (a timeout, a rate limit).
- `Permanent(...)`: retrying won't help (missing file, invalid input).
- anything else: unexpected. Treated as retryable, since many unexpected errors
  are transient, but bounded by the job's max_attempts, so a bug stops after a
  few attempts with its error recorded.

Long-running handlers call `ctx.heartbeat()` now and then to extend their lease,
so a slow job isn't mistaken for an abandoned one.

The clock and the sleep function are injected. Tests pass fake ones and
simulate minutes of retries and lease expiries in milliseconds.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from jobrunner.jobs import Job, RetryPolicy, State, TransitionError
from jobrunner.store import JobStore

log = logging.getLogger("jobrunner.worker")

Handler = Callable[[Any, "JobContext"], Any]


class Retryable(Exception):
    """The job failed for a reason that may go away; try it again later."""


class Permanent(Exception):
    """The job can never succeed as submitted; stop retrying."""


def system_clock_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass
class JobContext:
    """What a handler can see and do while it runs."""

    job_id: int
    attempt: int
    _store: JobStore
    _clock: Callable[[], int]
    _lease_ms: int

    def heartbeat(self) -> None:
        """Extend this attempt's lease. Raises TransitionError if the lease was already lost."""
        self._store.extend_lease(self.job_id, self.attempt, self._clock(), self._lease_ms)


@dataclass(frozen=True)
class Outcome:
    """What happened to one claimed job."""

    job: Job
    status: str  # "succeeded", "retrying", "failed", or "lease_lost"
    detail: str = ""


class Worker:
    def __init__(
        self,
        store: JobStore,
        handlers: Mapping[str, Handler],
        *,
        worker_id: str = "worker-1",
        lease_ms: int = 30_000,
        poll_interval_ms: int = 1_000,
        policy: RetryPolicy = RetryPolicy(),
        clock: Callable[[], int] = system_clock_ms,
        sleep: Callable[[float], None] = time.sleep,
        rand: Callable[[], float] = random.random,
    ) -> None:
        if not handlers:
            raise ValueError("a worker needs at least one handler")
        self.store = store
        self.handlers = dict(handlers)
        self.worker_id = worker_id
        self.lease_ms = lease_ms
        self.poll_interval_ms = poll_interval_ms
        self.policy = policy
        self.clock = clock
        self.sleep = sleep
        self.rand = rand
        self._stop = threading.Event()

    def stop(self) -> None:
        """Ask the loop to stop after the job it's running, if any. Safe to call from a signal handler."""
        self._stop.set()

    def run(self, max_jobs: int | None = None) -> list[Outcome]:
        """Process jobs until stop() is called, or until max_jobs jobs were handled."""
        outcomes: list[Outcome] = []
        log.info("worker %s started; handles %s", self.worker_id, ", ".join(sorted(self.handlers)))
        while not self._stop.is_set() and (max_jobs is None or len(outcomes) < max_jobs):
            outcome = self.run_once()
            if outcome is None:
                self.sleep(self.poll_interval_ms / 1000)
            else:
                outcomes.append(outcome)
        log.info("worker %s stopped after %d jobs", self.worker_id, len(outcomes))
        return outcomes

    def run_once(self) -> Outcome | None:
        """Recover abandoned jobs, then claim and run one job. Returns None if nothing was runnable."""
        for job in self.store.recover_expired(self.clock(), self.policy, self.rand):
            log.warning("recovered job %d: %s (now %s)", job.job_id, job.last_error, job.state.value)

        job = self.store.claim_next(self.worker_id, self.clock(), self.lease_ms, kinds=tuple(self.handlers))
        if job is None:
            return None
        log.info("job %d (%s) attempt %d/%d started", job.job_id, job.kind, job.attempts, job.max_attempts)
        ctx = JobContext(job.job_id, job.token, self.store, self.clock, self.lease_ms)
        handler = self.handlers[job.kind]

        try:
            result = handler(job.payload, ctx)
        except Permanent as exc:
            return self._record_failure(job, f"{exc}", retryable=False)
        except Retryable as exc:
            return self._record_failure(job, f"{exc}", retryable=True)
        except Exception as exc:  # unexpected: retry, but only up to max_attempts
            log.exception("job %d raised an unexpected error", job.job_id)
            return self._record_failure(job, f"{type(exc).__name__}: {exc}", retryable=True)
        # BaseException (KeyboardInterrupt, SystemExit) is deliberately not caught: the
        # process is going away, the job stays "running", and its lease expiry recovers it.

        try:
            done = self.store.complete(job.job_id, job.token, self.clock(), result)
        except TransitionError as exc:
            # The lease ran out while the handler was working. Another worker may already
            # be running this job again, so this result must be dropped: at-least-once.
            log.warning("job %d finished after losing its lease; result discarded: %s", job.job_id, exc)
            return Outcome(self.store.get(job.job_id), "lease_lost", str(exc))
        except ValueError as exc:  # the handler returned something that isn't JSON
            return self._record_failure(job, f"handler returned a result that can't be stored: {exc}",
                                        retryable=False)
        log.info("job %d succeeded", job.job_id)
        return Outcome(done, "succeeded")

    def _record_failure(self, job: Job, error: str, *, retryable: bool) -> Outcome:
        try:
            after = self.store.fail(job.job_id, job.token, self.clock(), error, retryable=retryable,
                                    policy=self.policy, rand=self.rand)
        except TransitionError as exc:
            log.warning("job %d failed after losing its lease; failure not recorded: %s", job.job_id, exc)
            return Outcome(self.store.get(job.job_id), "lease_lost", str(exc))
        status = "retrying" if after.state is State.QUEUED else "failed"
        log.warning("job %d attempt %d failed (%s): %s", job.job_id, job.attempts, status, error)
        return Outcome(after, status, error)
