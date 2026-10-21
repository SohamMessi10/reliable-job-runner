"""The job state machine, as pure functions.

Every transition takes a Job and returns a new Job; nothing is changed in place
and nothing reads the clock. The current time (`now`, integer milliseconds) is
always passed in, so tests can jump past a lease expiry instantly and every
result is deterministic. A transition that isn't allowed raises
TransitionError and leaves the job exactly as it was.

    queued ──claim──► running ──complete──► succeeded
      ▲                  │
      └── retry ◄── fail / lease expired ──► failed (no attempts left, or permanent)

Each claim increments `attempts`, and that number is the fencing token: a
worker must present it to complete, fail, or extend the job, so a worker whose
lease already expired can't overwrite a newer attempt.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, replace
from enum import Enum
from typing import Any, Callable

MAX_ERROR_LENGTH = 2_000


class State(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

    @property
    def terminal(self) -> bool:
        return self in (State.SUCCEEDED, State.FAILED)


class TransitionError(Exception):
    """The requested change isn't allowed from the job's current state."""


@dataclass(frozen=True)
class RetryPolicy:
    """How many attempts a job gets, and how long to wait between them (milliseconds)."""

    max_attempts: int = 5
    base_delay_ms: int = 1_000
    max_delay_ms: int = 60_000

    def __post_init__(self) -> None:
        for name in ("max_attempts", "base_delay_ms", "max_delay_ms"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"{name} must be a whole number, got {value!r}")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay_ms < 0 or self.max_delay_ms < self.base_delay_ms:
            raise ValueError("delays must satisfy 0 <= base_delay_ms <= max_delay_ms")

    def backoff(self, attempt: int, rand: Callable[[], float] = random.random) -> int:
        """Delay before retrying after attempt `attempt` failed: exponential, with full jitter.

        The ceiling doubles each attempt (base, 2×base, 4×base, ...) up to max_delay_ms,
        and the actual delay is a random point between 0 and that ceiling, so many jobs
        that failed together don't all retry at the same instant.
        """
        if attempt < 1:
            raise ValueError("attempt numbers start at 1")
        ceiling = min(self.max_delay_ms, self.base_delay_ms * 2 ** min(attempt - 1, 62))
        fraction = rand()
        if not 0.0 <= fraction < 1.0:
            raise ValueError(f"rand() must return a number in [0, 1), got {fraction!r}")
        return int(ceiling * fraction)


@dataclass(frozen=True)
class Job:
    job_id: int
    kind: str
    payload: Any
    state: State
    attempts: int
    max_attempts: int
    run_after: int  # not claimable before this time (ms)
    lease_owner: str | None = None
    lease_expires_at: int | None = None
    result: Any = None
    last_error: str | None = None
    idempotency_key: str | None = None

    @property
    def token(self) -> int:
        """The fencing token of the current attempt."""
        return self.attempts


def submit(job_id: int, kind: str, payload: Any, now: int, policy: RetryPolicy = RetryPolicy(),
           idempotency_key: str | None = None) -> Job:
    """A new job, ready to run immediately."""
    if not isinstance(kind, str) or not kind.strip() or kind != kind.strip():
        raise ValueError(f"job kind must be non-empty text without surrounding spaces, got {kind!r}")
    if idempotency_key is not None and (not isinstance(idempotency_key, str) or not idempotency_key.strip()):
        raise ValueError("idempotency key must be non-empty text when given")
    _check_time(now)
    return Job(job_id, kind, payload, State.QUEUED, attempts=0, max_attempts=policy.max_attempts,
               run_after=now, idempotency_key=idempotency_key)


def claim(job: Job, worker: str, now: int, lease_ms: int) -> Job:
    """Start an attempt: the worker holds the job until now + lease_ms."""
    _check_time(now)
    _check_worker(worker)
    _check_lease(lease_ms)
    if job.state is not State.QUEUED:
        raise TransitionError(f"job {job.job_id} is {job.state.value}, not queued")
    if now < job.run_after:
        raise TransitionError(f"job {job.job_id} can't run until {job.run_after} (now {now})")
    if job.attempts >= job.max_attempts:  # unreachable via these functions; guards stored data
        raise TransitionError(f"job {job.job_id} has no attempts left")
    return replace(job, state=State.RUNNING, attempts=job.attempts + 1,
                   lease_owner=worker, lease_expires_at=now + lease_ms)


def extend_lease(job: Job, token: int, now: int, lease_ms: int) -> Job:
    """Keep holding a long-running job: the lease now ends at now + lease_ms."""
    _check_time(now)
    _check_lease(lease_ms)
    _check_owner(job, token, now)
    return replace(job, lease_expires_at=now + lease_ms)


def complete(job: Job, token: int, now: int, result: Any = None) -> Job:
    """The attempt succeeded."""
    _check_time(now)
    _check_owner(job, token, now)
    return replace(job, state=State.SUCCEEDED, lease_owner=None, lease_expires_at=None,
                   result=result, last_error=None)


def fail(job: Job, token: int, now: int, error: str, *, retryable: bool, policy: RetryPolicy = RetryPolicy(),
         rand: Callable[[], float] = random.random) -> Job:
    """The attempt failed. Retry later if allowed, otherwise mark the job failed."""
    _check_time(now)
    _check_owner(job, token, now)
    return _after_failure(job, now, _clean_error(error), retryable, policy, rand)


def expire_lease(job: Job, now: int, policy: RetryPolicy = RetryPolicy(),
                 rand: Callable[[], float] = random.random) -> Job:
    """Recover a job whose worker stopped responding. Counts as a retryable failure."""
    _check_time(now)
    if job.state is not State.RUNNING:
        raise TransitionError(f"job {job.job_id} is {job.state.value}, not running")
    if now < job.lease_expires_at:
        raise TransitionError(f"job {job.job_id}'s lease is still valid until {job.lease_expires_at}")
    error = f"lease held by {job.lease_owner} expired at {job.lease_expires_at} during attempt {job.attempts}"
    return _after_failure(job, now, error, True, policy, rand)


def check_invariants(job: Job) -> None:
    """Raise AssertionError if a job is in a state the lifecycle can't produce."""
    assert 0 <= job.attempts <= job.max_attempts, f"attempts {job.attempts} outside 0..{job.max_attempts}"
    running = job.state is State.RUNNING
    assert (job.lease_owner is not None) == running, "only a running job has a lease owner"
    assert (job.lease_expires_at is not None) == running, "only a running job has a lease expiry"
    if running:
        assert job.attempts >= 1, "a running job has started at least one attempt"
    if job.state is State.QUEUED:
        # Otherwise claim() refuses it and the job waits forever, neither running nor failing.
        assert job.attempts < job.max_attempts, "a queued job always has an attempt left"
    if job.state is State.FAILED:
        assert job.last_error, "a failed job records why"
    if job.state is State.SUCCEEDED:
        assert job.last_error is None, "a succeeded job has no error"


# --- helpers ------------------------------------------------------------------


def _after_failure(job: Job, now: int, error: str, retryable: bool, policy: RetryPolicy,
                   rand: Callable[[], float]) -> Job:
    released = replace(job, lease_owner=None, lease_expires_at=None, last_error=error)
    if retryable and job.attempts < job.max_attempts:
        return replace(released, state=State.QUEUED, run_after=now + policy.backoff(job.attempts, rand))
    return replace(released, state=State.FAILED)


def _check_owner(job: Job, token: int, now: int) -> None:
    if job.state is not State.RUNNING:
        raise TransitionError(f"job {job.job_id} is {job.state.value}, not running")
    if token != job.attempts:
        raise TransitionError(
            f"stale token {token} for job {job.job_id}: attempt {job.attempts} owns it now"
        )
    if now >= job.lease_expires_at:
        # The lease ran out; another worker may already be entitled to the job.
        raise TransitionError(f"lease on job {job.job_id} expired at {job.lease_expires_at} (now {now})")


def _check_time(now: int) -> None:
    if isinstance(now, bool) or not isinstance(now, int) or now < 0:
        raise ValueError(f"time must be a non-negative integer number of milliseconds, got {now!r}")


def _check_lease(lease_ms: int) -> None:
    if isinstance(lease_ms, bool) or not isinstance(lease_ms, int) or lease_ms < 1:
        raise ValueError(f"lease must be a positive integer number of milliseconds, got {lease_ms!r}")


def _check_worker(worker: str) -> None:
    if not isinstance(worker, str) or not worker.strip():
        raise ValueError(f"worker id must be non-empty text, got {worker!r}")


def _clean_error(error: str) -> str:
    text = str(error).strip() or "unknown error"
    return text if len(text) <= MAX_ERROR_LENGTH else text[: MAX_ERROR_LENGTH - 1] + "…"
