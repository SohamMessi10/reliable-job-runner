"""Jobs in a SQLite file, with claiming that is safe when workers race.

The rules of the lifecycle live in jobs.py as pure functions. This module only
loads a job, applies one of those functions, and writes the result back, all
inside one `BEGIN IMMEDIATE` transaction:

- IMMEDIATE takes the database's write lock before reading. Two workers can't
  both read "job 7 is queued" and both claim it: the second one waits, then
  sees that job 7 is already running.
- Every UPDATE also repeats what was read (`WHERE state = ? AND attempts = ?`)
  and checks that exactly one row changed. If anything ever bypassed the lock,
  the write fails loudly instead of overwriting someone else's work.

The schema's CHECK constraints mirror the invariants in jobs.check_invariants,
so even a row written by another tool can't describe an impossible job.

Times are integer milliseconds passed in by the caller, never read here.
"""

from __future__ import annotations

import json
import random
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from jobrunner import jobs
from jobrunner.jobs import Job, RetryPolicy, State, TransitionError

SCHEMA_VERSION = 1
LOCK_WAIT_SECONDS = 5.0

_SCHEMA = """
CREATE TABLE jobs (
    job_id           INTEGER PRIMARY KEY,
    kind             TEXT NOT NULL CHECK (length(kind) > 0 AND kind = trim(kind)),
    payload          TEXT NOT NULL CHECK (json_valid(payload)),
    state            TEXT NOT NULL CHECK (state IN ('queued', 'running', 'succeeded', 'failed')),
    attempts         INTEGER NOT NULL CHECK (attempts >= 0),
    max_attempts     INTEGER NOT NULL CHECK (max_attempts >= 1),
    run_after        INTEGER NOT NULL CHECK (run_after >= 0),
    lease_owner      TEXT,
    lease_expires_at INTEGER,
    result           TEXT CHECK (result IS NULL OR json_valid(result)),
    last_error       TEXT,
    idempotency_key  TEXT UNIQUE CHECK (idempotency_key IS NULL OR length(trim(idempotency_key)) > 0),
    created_at       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL,
    CHECK (attempts <= max_attempts),
    -- Only a running job holds a lease, and it has started at least one attempt.
    CHECK ((state = 'running') = (lease_owner IS NOT NULL)),
    CHECK ((state = 'running') = (lease_expires_at IS NOT NULL)),
    CHECK (state <> 'running' OR attempts >= 1),
    -- A queued job always has an attempt left, or it would wait forever.
    CHECK (state <> 'queued' OR attempts < max_attempts),
    CHECK (state <> 'failed' OR last_error IS NOT NULL),
    CHECK (state <> 'succeeded' OR last_error IS NULL)
) STRICT;

-- Finding the next runnable job only has to look at queued rows.
CREATE INDEX jobs_runnable ON jobs (run_after, job_id) WHERE state = 'queued';
-- Finding expired leases only has to look at running rows.
CREATE INDEX jobs_leases ON jobs (lease_expires_at) WHERE state = 'running';
"""


class StoreError(Exception):
    """The database file is missing, not a job runner file, or a write didn't apply."""


class IdempotencyConflict(Exception):
    """An idempotency key was reused for a different request."""


class JobNotFound(KeyError):
    """No job has this id."""


class JobStore:
    """Jobs in one SQLite file. Use `JobStore.open(path)` and close it when done."""

    def __init__(self, conn: sqlite3.Connection, path: Path) -> None:
        self._conn = conn
        self.path = path

    @classmethod
    def open(cls, path: str | Path, *, create: bool = False) -> JobStore:
        """Open a job file. With create=True, make a new one if it doesn't exist yet."""
        path = Path(path)
        exists = path.is_file()
        if not exists and not create:
            raise StoreError(f"{path} does not exist; pass create=True to make a new job file")
        conn = _connect(path, mode="rw" if exists else "rwc")
        try:
            if exists:
                version = conn.execute("PRAGMA user_version").fetchone()[0]
                tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_schema WHERE type = 'table'")}
                if version != SCHEMA_VERSION or "jobs" not in tables:
                    raise StoreError(f"{path} is not a job runner file (schema version {version})")
            else:
                # WAL lets readers (status checks) continue while a worker writes. The setting is
                # stored in the file, and it can't be changed inside a transaction.
                conn.execute("PRAGMA journal_mode = WAL")
                conn.execute("BEGIN IMMEDIATE")
                for statement in _SCHEMA.split(";"):
                    if statement.strip():
                        conn.execute(statement)
                conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                conn.execute("COMMIT")
        except sqlite3.DatabaseError as exc:
            conn.close()
            if not exists:
                _remove_database(path)
            raise StoreError(f"{path} is not a SQLite database: {exc}") from None
        except BaseException:
            conn.close()
            if not exists:
                _remove_database(path)
            raise
        return cls(conn, path)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> JobStore:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # --- submitting -----------------------------------------------------------

    def submit(self, kind: str, payload: Any, now: int, *, policy: RetryPolicy = RetryPolicy(),
               idempotency_key: str | None = None) -> tuple[Job, bool]:
        """Add a job. Returns (job, created).

        With an idempotency key that was used before for the same kind and payload, the
        existing job is returned and created is False, so retrying a submission is safe.
        Reusing a key for a different request raises IdempotencyConflict.
        """
        payload_text = _to_json(payload, "payload")
        with self._transaction() as conn:
            if idempotency_key is not None:
                row = conn.execute("SELECT * FROM jobs WHERE idempotency_key = ?", (idempotency_key,)).fetchone()
                if row is not None:
                    existing = _row_to_job(row)
                    if existing.kind != kind or row["payload"] != payload_text:
                        raise IdempotencyConflict(
                            f"idempotency key {idempotency_key!r} was already used for job "
                            f"{existing.job_id} with a different kind or payload"
                        )
                    return existing, False
            job_id = conn.execute("SELECT coalesce(max(job_id), 0) + 1 FROM jobs").fetchone()[0]
            job = jobs.submit(job_id, kind, payload, now, policy, idempotency_key)
            conn.execute(
                "INSERT INTO jobs (job_id, kind, payload, state, attempts, max_attempts, run_after, "
                "idempotency_key, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (job.job_id, job.kind, payload_text, job.state.value, job.attempts, job.max_attempts,
                 job.run_after, job.idempotency_key, now, now),
            )
        return job, True

    # --- reading --------------------------------------------------------------

    def get(self, job_id: int) -> Job:
        row = self._conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        if row is None:
            raise JobNotFound(f"there is no job {job_id}")
        return _row_to_job(row)

    def list(self, state: State | None = None, limit: int = 100) -> list[Job]:
        if state is None:
            rows = self._conn.execute("SELECT * FROM jobs ORDER BY job_id LIMIT ?", (limit,))
        else:
            rows = self._conn.execute("SELECT * FROM jobs WHERE state = ? ORDER BY job_id LIMIT ?",
                                      (State(state).value, limit))
        return [_row_to_job(r) for r in rows]

    def counts(self) -> dict[str, int]:
        found = dict(self._conn.execute("SELECT state, count(*) FROM jobs GROUP BY state").fetchall())
        return {s.value: found.get(s.value, 0) for s in State}

    # --- the lifecycle, one transaction per step ----------------------------------

    def claim_next(self, worker: str, now: int, lease_ms: int, kinds: tuple[str, ...] | None = None) -> Job | None:
        """Claim the job that has been runnable longest, or return None if none is runnable."""
        query = "SELECT * FROM jobs WHERE state = 'queued' AND run_after <= ?"
        params: list[Any] = [now]
        if kinds:
            query += f" AND kind IN ({', '.join('?' * len(kinds))})"
            params.extend(kinds)
        query += " ORDER BY run_after, job_id LIMIT 1"
        with self._transaction() as conn:
            row = conn.execute(query, params).fetchone()
            if row is None:
                return None
            before = _row_to_job(row)
            after = jobs.claim(before, worker, now, lease_ms)
            _save(conn, before, after, now)
        return after

    def extend_lease(self, job_id: int, token: int, now: int, lease_ms: int) -> Job:
        return self._apply(job_id, now, lambda job: jobs.extend_lease(job, token, now, lease_ms))

    def complete(self, job_id: int, token: int, now: int, result: Any = None) -> Job:
        # A result that isn't JSON makes the save raise inside the transaction, so nothing changes.
        return self._apply(job_id, now, lambda job: jobs.complete(job, token, now, result))

    def fail(self, job_id: int, token: int, now: int, error: str, *, retryable: bool,
             policy: RetryPolicy = RetryPolicy(), rand: Callable[[], float] = random.random) -> Job:
        return self._apply(job_id, now, lambda job: jobs.fail(job, token, now, error, retryable=retryable,
                                                              policy=policy, rand=rand))

    def recover_expired(self, now: int, policy: RetryPolicy = RetryPolicy(),
                        rand: Callable[[], float] = random.random) -> list[Job]:
        """Return every job whose lease has run out to the queue (or fail it if no attempts are left)."""
        recovered = []
        with self._transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE state = 'running' AND lease_expires_at <= ? ORDER BY job_id", (now,)
            ).fetchall()
            for row in rows:
                before = _row_to_job(row)
                after = jobs.expire_lease(before, now, policy, rand)
                _save(conn, before, after, now)
                recovered.append(after)
        return recovered

    # --- internals ------------------------------------------------------------

    def _apply(self, job_id: int, now: int, transition: Callable[[Job], Job]) -> Job:
        with self._transaction() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            if row is None:
                raise JobNotFound(f"there is no job {job_id}")
            before = _row_to_job(row)
            after = transition(before)  # raises TransitionError, which rolls back
            _save(conn, before, after, now)
        return after

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield self._conn
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        self._conn.execute("COMMIT")


def _save(conn: sqlite3.Connection, before: Job, after: Job, now: int) -> None:
    cursor = conn.execute(
        "UPDATE jobs SET state = ?, attempts = ?, run_after = ?, lease_owner = ?, lease_expires_at = ?, "
        "result = ?, last_error = ?, updated_at = ? "
        "WHERE job_id = ? AND state = ? AND attempts = ?",
        (after.state.value, after.attempts, after.run_after, after.lease_owner, after.lease_expires_at,
         None if after.result is None else _to_json(after.result, "result"), after.last_error, now,
         before.job_id, before.state.value, before.attempts),
    )
    if cursor.rowcount != 1:
        # Unreachable while every write goes through BEGIN IMMEDIATE; this is the backstop.
        raise StoreError(f"job {before.job_id} changed underneath this update; nothing was written")


def _row_to_job(row: sqlite3.Row) -> Job:
    return Job(
        job_id=row["job_id"], kind=row["kind"], payload=json.loads(row["payload"]), state=State(row["state"]),
        attempts=row["attempts"], max_attempts=row["max_attempts"], run_after=row["run_after"],
        lease_owner=row["lease_owner"], lease_expires_at=row["lease_expires_at"],
        result=None if row["result"] is None else json.loads(row["result"]),
        last_error=row["last_error"], idempotency_key=row["idempotency_key"],
    )


def _to_json(value: Any, what: str) -> str:
    try:
        # sort_keys: the same payload always has the same text, which the idempotency check relies on.
        # allow_nan=False: NaN and Infinity aren't valid JSON, and SQLite's json_valid rejects them.
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{what} must be JSON (numbers, text, lists, objects): {exc}") from None


def _remove_database(path: Path) -> None:
    # A WAL database can leave two companion files next to it.
    for leftover in (path, path.with_name(path.name + "-wal"), path.with_name(path.name + "-shm")):
        leftover.unlink(missing_ok=True)


def _connect(path: Path, mode: str) -> sqlite3.Connection:
    uri = f"{path.resolve().as_uri()}?mode={mode}"
    # timeout: wait up to 5 s for another writer's lock instead of failing at once.
    # isolation_level=None: no hidden transactions; this module issues BEGIN/COMMIT itself.
    conn = sqlite3.connect(uri, uri=True, timeout=LOCK_WAIT_SECONDS, isolation_level=None)
    conn.row_factory = sqlite3.Row
    return conn


__all__ = ["IdempotencyConflict", "JobNotFound", "JobStore", "StoreError", "TransitionError"]
