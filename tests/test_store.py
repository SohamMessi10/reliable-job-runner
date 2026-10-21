import json
import sqlite3
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from jobrunner import (
    IdempotencyConflict,
    JobNotFound,
    JobStore,
    RetryPolicy,
    State,
    StoreError,
    TransitionError,
    check_invariants,
)

LEASE = 30_000
HALF = lambda: 0.5  # noqa: E731
POLICY = RetryPolicy(max_attempts=3, base_delay_ms=1_000, max_delay_ms=10_000)
RACE_WORKER = Path(__file__).resolve().parent / "race_worker.py"


@pytest.fixture
def path(tmp_path):
    return tmp_path / "jobs.db"


@pytest.fixture
def store(path):
    with JobStore.open(path, create=True) as store:
        yield store


def raw(path):
    return sqlite3.connect(path, isolation_level=None)


# --- opening ----------------------------------------------------------------


def test_open_missing_file_without_create_makes_nothing(path):
    with pytest.raises(StoreError, match="does not exist"):
        JobStore.open(path)
    assert not path.exists()


def test_new_file_uses_wal_and_survives_reopening(path):
    with JobStore.open(path, create=True) as store:
        store.submit("email", {"to": "a@example.com"}, now=0)
    assert raw(path).execute("PRAGMA journal_mode").fetchone() == ("wal",)
    with JobStore.open(path) as store:
        assert store.get(1).payload == {"to": "a@example.com"}


def test_open_rejects_other_files(path, tmp_path):
    path.write_text("not a database " * 100)
    with pytest.raises(StoreError, match="not a SQLite database"):
        JobStore.open(path)
    other = tmp_path / "other.db"
    raw(other).execute("CREATE TABLE notes (t TEXT)")
    with pytest.raises(StoreError, match="not a job runner file"):
        JobStore.open(other)


# --- submitting -------------------------------------------------------------


def test_submit_and_get(store):
    job, created = store.submit("summarize_csv", {"path": "data.csv"}, now=100, policy=POLICY)
    assert created and job.job_id == 1
    assert store.get(1) == job
    assert (job.state, job.run_after, job.max_attempts) == (State.QUEUED, 100, 3)


def test_ids_increase(store):
    assert [store.submit("k", i, now=0)[0].job_id for i in range(3)] == [1, 2, 3]


def test_same_idempotency_key_and_request_returns_the_existing_job(store):
    first, created = store.submit("charge", {"amount": 500, "user": 7}, now=0, idempotency_key="req-42")
    again, created_again = store.submit("charge", {"user": 7, "amount": 500}, now=99, idempotency_key="req-42")
    assert created and not created_again
    assert again == first  # key order in the payload doesn't matter
    assert store.counts()["queued"] == 1


def test_reusing_a_key_for_a_different_request_is_refused(store):
    store.submit("charge", {"amount": 500}, now=0, idempotency_key="req-42")
    with pytest.raises(IdempotencyConflict, match="already used for job 1"):
        store.submit("charge", {"amount": 9_999}, now=1, idempotency_key="req-42")
    with pytest.raises(IdempotencyConflict):
        store.submit("refund", {"amount": 500}, now=1, idempotency_key="req-42")
    assert store.counts()["queued"] == 1


def test_idempotent_return_reflects_current_state(store):
    store.submit("k", {}, now=0, idempotency_key="x")
    job = store.claim_next("w", now=1, lease_ms=LEASE)
    store.complete(job.job_id, job.token, now=2, result="ok")
    again, created = store.submit("k", {}, now=3, idempotency_key="x")
    assert not created and again.state is State.SUCCEEDED and again.result == "ok"


@pytest.mark.parametrize("payload", [float("nan"), {"x": float("inf")}, {1, 2}, object()])
def test_payload_must_be_json(store, payload):
    with pytest.raises(ValueError, match="must be JSON"):
        store.submit("k", payload, now=0)
    assert store.counts()["queued"] == 0


@pytest.mark.parametrize("key", ["", "   "])
def test_blank_idempotency_keys_are_refused(store, key):
    with pytest.raises(ValueError, match="idempotency key"):
        store.submit("k", {}, now=0, idempotency_key=key)


# --- claiming ---------------------------------------------------------------


def test_claim_next_takes_the_job_waiting_longest(store):
    store.submit("k", "late", now=500)
    store.submit("k", "early", now=100)
    assert store.claim_next("w", now=1_000, lease_ms=LEASE).payload == "early"


def test_claim_next_skips_jobs_that_are_not_due(store):
    store.submit("k", "future", now=5_000)
    assert store.claim_next("w", now=4_999, lease_ms=LEASE) is None
    assert store.claim_next("w", now=5_000, lease_ms=LEASE).payload == "future"


def test_claim_next_by_kind(store):
    store.submit("email", 1, now=0)
    store.submit("report", 2, now=0)
    assert store.claim_next("w", now=1, lease_ms=LEASE, kinds=("report",)).kind == "report"


def test_a_claimed_job_is_not_handed_out_twice(store):
    store.submit("k", {}, now=0)
    assert store.claim_next("a", now=1, lease_ms=LEASE) is not None
    assert store.claim_next("b", now=2, lease_ms=LEASE) is None


# --- completing, failing, recovering ----------------------------------------


def test_complete_stores_the_result(store):
    store.submit("k", {}, now=0)
    job = store.claim_next("w", now=1, lease_ms=LEASE)
    store.complete(job.job_id, job.token, now=2, result={"rows": 42, "ok": True})
    assert store.get(job.job_id).result == {"rows": 42, "ok": True}


def test_result_must_be_json_and_nothing_changes_if_it_is_not(store):
    store.submit("k", {}, now=0)
    job = store.claim_next("w", now=1, lease_ms=LEASE)
    with pytest.raises(ValueError, match="must be JSON"):
        store.complete(job.job_id, job.token, now=2, result={"when": object()})
    assert store.get(job.job_id) == job


def test_retry_then_permanent_failure(store):
    store.submit("k", {}, now=0, policy=POLICY)
    job = store.claim_next("w", now=0, lease_ms=LEASE)
    retry = store.fail(job.job_id, job.token, now=10, error="timeout", retryable=True, policy=POLICY, rand=HALF)
    assert (retry.state, retry.run_after) == (State.QUEUED, 510)
    job = store.claim_next("w", now=510, lease_ms=LEASE)
    failed = store.fail(job.job_id, job.token, now=520, error="bad input", retryable=False)
    assert (store.get(1).state, store.get(1).last_error, failed.attempts) == (State.FAILED, "bad input", 2)


def test_recover_expired_returns_only_expired_jobs(store):
    for i in range(3):
        store.submit("k", i, now=0)
    a = store.claim_next("a", now=0, lease_ms=10_000)
    b = store.claim_next("b", now=0, lease_ms=50_000)
    recovered = store.recover_expired(now=20_000, rand=HALF)
    assert [j.job_id for j in recovered] == [a.job_id]
    assert store.get(a.job_id).state is State.QUEUED
    assert store.get(b.job_id).state is State.RUNNING


def test_stale_worker_is_refused_and_nothing_changes(store):
    store.submit("k", {}, now=0)
    old = store.claim_next("slow", now=0, lease_ms=LEASE)
    store.recover_expired(now=LEASE, rand=HALF)
    new = store.claim_next("fast", now=LEASE + 1_000, lease_ms=LEASE)
    with pytest.raises(TransitionError, match="stale token 1"):
        store.complete(old.job_id, old.token, now=LEASE + 1_001, result="from slow")
    assert store.get(new.job_id) == new


def test_unknown_job(store):
    with pytest.raises(JobNotFound):
        store.get(99)
    with pytest.raises(JobNotFound):
        store.complete(99, 1, now=0)


def test_list_and_counts(store):
    for i in range(3):
        store.submit("k", i, now=0)
    store.claim_next("w", now=0, lease_ms=LEASE)
    assert store.counts() == {"queued": 2, "running": 1, "succeeded": 0, "failed": 0}
    assert [j.job_id for j in store.list(State.QUEUED)] == [2, 3]
    assert len(store.list()) == 3


# --- the database protects itself -------------------------------------------


@pytest.mark.parametrize(
    "update",
    [
        "state = 'running'",  # running without a lease
        "lease_owner = 'w'",  # a lease on a queued job
        "attempts = max_attempts",  # queued with no attempts left
        "state = 'failed'",  # failed without an error
        "attempts = -1",
        "state = 'paused'",
        "payload = 'not json'",
        "kind = ' padded'",
    ],
)
def test_schema_rejects_impossible_jobs(store, path, update):
    store.submit("k", {}, now=0)
    with pytest.raises(sqlite3.IntegrityError):
        raw(path).execute(f"UPDATE jobs SET {update} WHERE job_id = 1")


def test_duplicate_idempotency_key_is_refused_by_the_database(store, path):
    store.submit("k", {}, now=0, idempotency_key="x")
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
        raw(path).execute(
            "INSERT INTO jobs (job_id, kind, payload, state, attempts, max_attempts, run_after, "
            "idempotency_key, created_at, updated_at) VALUES (2, 'k', '{}', 'queued', 0, 3, 0, 'x', 0, 0)"
        )


def test_update_is_refused_if_the_row_changed_underneath(store, path):
    # Simulate a write that bypassed the lock: the job changes between our read and our write.
    from jobrunner import jobs as pure
    from jobrunner import store as store_module

    store.submit("k", {}, now=0)
    before = store.get(1)
    raw(path).execute("UPDATE jobs SET state = 'running', attempts = 1, lease_owner = 'other', lease_expires_at = 99")
    after = pure.claim(before, "me", 0, LEASE)
    store._conn.execute("BEGIN IMMEDIATE")
    with pytest.raises(StoreError, match="changed underneath"):
        store_module._save(store._conn, before, after, now=0)
    store._conn.execute("ROLLBACK")
    assert store.get(1).lease_owner == "other"


# --- racing workers in separate processes -----------------------------------


def run_race(path, signals, workers, n_jobs):
    """Start `workers` competing processes on `n_jobs` jobs; return (claims per worker, all claims)."""
    import time

    with JobStore.open(path, create=True) as store:
        for i in range(n_jobs):
            store.submit("k", i, now=0)
    signals.mkdir()
    procs = [
        subprocess.Popen([sys.executable, str(RACE_WORKER), str(path), f"w{i}", str(signals)],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        for i in range(workers)
    ]
    deadline = time.monotonic() + 60
    while len(list(signals.glob("ready-*"))) < workers:  # wait until every worker is ready...
        assert time.monotonic() < deadline, "workers never became ready"
        assert all(p.poll() is None for p in procs), "a worker exited early"
        time.sleep(0.005)
    (signals / "go").touch()  # ...then release them all at once
    per_worker, claims = {}, []
    for i, proc in enumerate(procs):
        out, err = proc.communicate(timeout=120)
        assert proc.returncode == 0, err
        per_worker[f"w{i}"] = json.loads(out)
        claims.extend(per_worker[f"w{i}"])
    return per_worker, claims


@pytest.mark.parametrize("workers, n_jobs", [(4, 120), (8, 160)])
def test_racing_processes_claim_each_job_exactly_once(path, tmp_path, workers, n_jobs):
    per_worker, claims = run_race(path, tmp_path / "signals", workers, n_jobs)

    counts = Counter(claims)
    assert sorted(counts) == list(range(1, n_jobs + 1))  # every job was claimed...
    assert max(counts.values()) == 1  # ...by exactly one worker
    # The race was real: several workers competed and none drained the queue alone. (Not
    # "every worker": on a machine with few CPUs the scheduler can starve one process.)
    assert sum(1 for ids in per_worker.values() if ids) >= 3
    assert max(len(ids) for ids in per_worker.values()) <= 0.6 * n_jobs
    with JobStore.open(path) as store:
        assert store.counts()["succeeded"] == n_jobs
        assert all(j.attempts == 1 for j in store.list(limit=n_jobs))


# --- random histories stay valid in the database (Hypothesis) ---------------

steps = st.lists(
    st.one_of(
        st.tuples(st.just("submit"), st.sampled_from([None, "k1", "k2"])),
        st.tuples(st.just("claim"), st.sampled_from(["a", "b"])),
        st.tuples(st.just("complete"), st.integers(1, 4), st.integers(0, 5)),
        st.tuples(st.just("fail"), st.integers(1, 4), st.integers(0, 5), st.booleans()),
        st.tuples(st.just("wait"), st.one_of(st.integers(0, 2_000), st.integers(LEASE, 2 * LEASE))),
        st.tuples(st.just("recover")),
    ),
    max_size=40,
)


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=60)
@given(steps)
def test_random_operations_keep_every_stored_job_valid(tmp_path, ops):
    path = tmp_path / f"h{abs(hash(str(ops)))}.db"
    for leftover in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        leftover.unlink(missing_ok=True)
    now = 0
    with JobStore.open(path, create=True) as store:
        for op in ops:
            try:
                if op[0] == "submit":
                    store.submit("k", {}, now, policy=POLICY, idempotency_key=op[1])
                elif op[0] == "claim":
                    store.claim_next(op[1], now, LEASE)
                elif op[0] == "complete":
                    store.complete(op[1], op[2], now, "ok")
                elif op[0] == "fail":
                    store.fail(op[1], op[2], now, "boom", retryable=op[3], policy=POLICY, rand=HALF)
                elif op[0] == "wait":
                    now += op[1]
                else:
                    store.recover_expired(now, POLICY, HALF)
            except (TransitionError, JobNotFound):
                pass
            for job in store.list(limit=1_000):
                check_invariants(job)
