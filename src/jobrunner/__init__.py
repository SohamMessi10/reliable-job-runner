"""Reliable Job Runner: a background job queue with retries, leases, and crash recovery."""

from jobrunner.jobs import (
    Job,
    RetryPolicy,
    State,
    TransitionError,
    check_invariants,
    claim,
    complete,
    expire_lease,
    extend_lease,
    fail,
    submit,
)
from jobrunner.store import SCHEMA_VERSION, IdempotencyConflict, JobNotFound, JobStore, StoreError

__all__ = [
    "SCHEMA_VERSION",
    "IdempotencyConflict",
    "JobNotFound",
    "JobStore",
    "StoreError",
    "Job",
    "RetryPolicy",
    "State",
    "TransitionError",
    "check_invariants",
    "claim",
    "complete",
    "expire_lease",
    "extend_lease",
    "fail",
    "submit",
]
__version__ = "0.1.0"
