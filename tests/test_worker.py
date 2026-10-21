import json

import pytest

from jobrunner import JobStore, Permanent, Retryable, RetryPolicy, State, Worker
from jobrunner.handlers import HANDLERS, summarize_csv

LEASE = 30_000
POLICY = RetryPolicy(max_attempts=3, base_delay_ms=1_000, max_delay_ms=10_000)


class FakeClock:
    """Time that only moves when a test (or a handler) moves it."""

    def __init__(self):
        self.now = 1_000
        self.slept = []

    def __call__(self):
        return self.now

    def advance(self, ms):
        self.now += ms

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += int(seconds * 1000)


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def store(tmp_path):
    with JobStore.open(tmp_path / "jobs.db", create=True) as store:
        yield store


def make_worker(store, clock, handlers, worker_id="w1"):
    return Worker(store, handlers, worker_id=worker_id, lease_ms=LEASE, poll_interval_ms=500,
                  policy=POLICY, clock=clock, sleep=clock.sleep, rand=lambda: 0.5)


def submit(store, clock, kind="echo", payload=None):
    return store.submit(kind, payload if payload is not None else {}, clock.now, policy=POLICY)[0]


# --- outcomes ---------------------------------------------------------------


def test_success_stores_the_result(store, clock):
    job = submit(store, clock, payload={"x": 2})
    outcome = make_worker(store, clock, {"echo": lambda p, ctx: {"doubled": p["x"] * 2}}).run_once()
    assert outcome.status == "succeeded"
    assert store.get(job.job_id).result == {"doubled": 4}


def test_retryable_error_is_retried_after_backoff_then_succeeds(store, clock):
    calls = []

    def flaky(payload, ctx):
        calls.append(ctx.attempt)
        if ctx.attempt == 1:
            raise Retryable("upstream timed out")
        return "ok"

    job = submit(store, clock)
    worker = make_worker(store, clock, {"echo": flaky})
    first = worker.run_once()
    assert (first.status, first.detail, first.job.run_after) == ("retrying", "upstream timed out", clock.now + 500)
    assert worker.run_once() is None  # still in backoff
    clock.advance(500)
    assert worker.run_once().status == "succeeded"
    assert calls == [1, 2]
    assert store.get(job.job_id).attempts == 2


def test_permanent_error_fails_without_retrying(store, clock):
    calls = []

    def bad_input(payload, ctx):
        calls.append(1)
        raise Permanent("customer id is missing")

    job = submit(store, clock)
    outcome = make_worker(store, clock, {"echo": bad_input}).run_once()
    assert outcome.status == "failed"
    assert (store.get(job.job_id).state, store.get(job.job_id).last_error) == (State.FAILED, "customer id is missing")
    assert calls == [1]


def test_unexpected_errors_are_retried_until_attempts_run_out(store, clock):
    job = submit(store, clock)
    worker = make_worker(store, clock, {"echo": lambda p, ctx: 1 / 0})
    statuses = []
    for _ in range(3):
        statuses.append(worker.run_once().status)
        clock.advance(10_000)
    assert statuses == ["retrying", "retrying", "failed"]
    final = store.get(job.job_id)
    assert (final.attempts, final.last_error) == (3, "ZeroDivisionError: division by zero")


def test_result_that_is_not_json_fails_permanently(store, clock):
    job = submit(store, clock)
    outcome = make_worker(store, clock, {"echo": lambda p, ctx: {1, 2}}).run_once()
    assert outcome.status == "failed"
    assert "can't be stored" in store.get(job.job_id).last_error


def test_worker_only_claims_kinds_it_can_handle(store, clock):
    email = submit(store, clock, kind="email")
    report = submit(store, clock, kind="report")
    outcome = make_worker(store, clock, {"report": lambda p, ctx: "done"}).run_once()
    assert outcome.job.job_id == report.job_id
    assert store.get(email.job_id).state is State.QUEUED


def test_a_worker_needs_handlers(store, clock):
    with pytest.raises(ValueError, match="at least one handler"):
        make_worker(store, clock, {})


# --- leases, crashes, and at-least-once -------------------------------------


def test_a_handler_that_outlives_its_lease_has_its_result_discarded_and_the_job_reruns(store, clock):
    runs = []

    def slow_then_fast(payload, ctx):
        runs.append(ctx.attempt)
        if ctx.attempt == 1:
            clock.advance(LEASE + 1)  # froze for longer than the lease
        return f"result of attempt {ctx.attempt}"

    job = submit(store, clock)
    first = make_worker(store, clock, {"echo": slow_then_fast}, "slow").run_once()
    assert first.status == "lease_lost"  # attempt 1 finished, but too late to count
    fast = make_worker(store, clock, {"echo": slow_then_fast}, "fast")
    assert fast.run_once() is None  # recovers the job; it then waits out its backoff...
    assert store.get(job.job_id).state is State.QUEUED
    clock.advance(500)  # ...which is counted from the moment of recovery
    second = fast.run_once()
    assert second.status == "succeeded"
    assert runs == [1, 2]  # the handler really ran twice: at-least-once, not exactly-once
    assert store.get(job.job_id).result == "result of attempt 2"


def test_heartbeat_keeps_a_long_job_alive(store, clock):
    def long_job(payload, ctx):
        for _ in range(5):
            clock.advance(LEASE // 2)  # 2.5 leases of work in total
            ctx.heartbeat()
        return "finished"

    submit(store, clock)
    assert make_worker(store, clock, {"echo": long_job}).run_once().status == "succeeded"


def test_without_heartbeats_the_same_long_job_loses_its_lease(store, clock):
    def long_job(payload, ctx):
        clock.advance(LEASE * 2)
        return "finished"

    submit(store, clock)
    assert make_worker(store, clock, {"echo": long_job}).run_once().status == "lease_lost"


def test_heartbeat_after_losing_the_lease_stops_the_handler(store, clock):
    from jobrunner import TransitionError

    seen = []

    def stalls_then_heartbeats(payload, ctx):
        clock.advance(LEASE + 1)
        try:
            ctx.heartbeat()
        except TransitionError as exc:
            seen.append(str(exc))
            raise
        return "unreachable"

    submit(store, clock)
    outcome = make_worker(store, clock, {"echo": stalls_then_heartbeats}).run_once()
    assert outcome.status == "lease_lost"  # the failure can't be recorded either: lease is gone
    assert "expired" in seen[0]


def test_a_crashed_worker_leaves_the_job_running_until_its_lease_expires(store, clock):
    def crash(payload, ctx):
        raise KeyboardInterrupt  # the process is being killed mid-job

    job = submit(store, clock)
    with pytest.raises(KeyboardInterrupt):
        make_worker(store, clock, {"echo": crash}, "doomed").run_once()
    assert store.get(job.job_id).state is State.RUNNING  # nobody recorded anything

    rescuer = make_worker(store, clock, {"echo": lambda p, ctx: "recovered"}, "rescuer")
    assert rescuer.run_once() is None  # lease still valid: not touched yet
    clock.advance(LEASE)  # the lease expires...
    assert rescuer.run_once() is None  # ...the rescuer recovers the job, which waits out its backoff
    assert "lease held by doomed expired" in store.get(job.job_id).last_error
    clock.advance(500)
    outcome = rescuer.run_once()
    assert (outcome.status, store.get(job.job_id).result) == ("succeeded", "recovered")
    assert "lease held by doomed expired" not in (store.get(job.job_id).last_error or "")  # success clears it


# --- the loop ---------------------------------------------------------------


def run_with_deadline(worker, max_turns=1_000, **kwargs):
    """Run the loop, failing (not hanging) if it spins far longer than a correct run could.

    Counts loop turns instead of using a thread with a timeout, because the store's SQLite
    connection belongs to this thread and must not be used from another one.
    """
    turns = {"n": 0}
    real_run_once = worker.run_once

    def counted_run_once():
        turns["n"] += 1
        if turns["n"] > max_turns:
            raise AssertionError(f"worker loop ran {max_turns} turns without stopping")
        return real_run_once()

    worker.run_once = counted_run_once
    return worker.run(**kwargs)



def test_run_processes_jobs_then_sleeps_when_idle(store, clock):
    for i in range(3):
        submit(store, clock, payload={"i": i})
    worker = make_worker(store, clock, {"echo": lambda p, ctx: p["i"]})
    outcomes = run_with_deadline(worker, max_jobs=3)
    assert [o.job.result for o in outcomes] == [0, 1, 2]
    assert clock.slept == []  # never idle while there was work

    def stop_after_two_naps(seconds):
        clock.slept.append(seconds)
        if len(clock.slept) == 2:
            worker.stop()

    worker.sleep = stop_after_two_naps
    assert run_with_deadline(worker) == []
    assert clock.slept == [0.5, 0.5]


def test_stop_lets_the_current_job_finish(store, clock):
    submit(store, clock)
    submit(store, clock)
    worker = make_worker(store, clock, {"echo": None})

    def handler(payload, ctx):
        worker.stop()  # e.g. Ctrl+C arrives while this job runs
        return "finished anyway"

    worker.handlers = {"echo": handler}
    outcomes = run_with_deadline(worker)
    assert [o.status for o in outcomes] == ["succeeded"]  # finished the current job, didn't start the next
    assert store.counts() == {"queued": 1, "running": 0, "succeeded": 1, "failed": 0}


# --- the demo handler -------------------------------------------------------


@pytest.fixture
def csv_file(tmp_path):
    path = tmp_path / "sales.csv"
    path.write_text("id,city,amount\n1,Boston,19.99\n2,Montréal,5.01\n3,Austin,-2.00\n", encoding="utf-8")
    return path


def test_summarize_csv_end_to_end(store, clock, csv_file, tmp_path):
    out = tmp_path / "summary.json"
    job = submit(store, clock, kind="summarize_csv", payload={"input": str(csv_file), "output": str(out)})
    assert make_worker(store, clock, HANDLERS).run_once().status == "succeeded"
    summary = json.loads(out.read_text())
    assert summary == store.get(job.job_id).result == {
        "rows": 3,
        "columns": ["id", "city", "amount"],
        "numeric": {"id": {"min": "1", "max": "3", "sum": "6"},
                    "amount": {"min": "-2.00", "max": "19.99", "sum": "23.00"}},
    }


def test_summarize_csv_is_idempotent(csv_file, tmp_path):
    out = tmp_path / "summary.json"
    payload = {"input": str(csv_file), "output": str(out)}
    summarize_csv(payload, ctx=None)
    first = out.read_bytes()
    summarize_csv(payload, ctx=None)  # what at-least-once delivery may do
    assert out.read_bytes() == first
    assert {p.name for p in tmp_path.iterdir()} == {"sales.csv", "summary.json"}  # no temp files left


@pytest.mark.parametrize(
    "setup, payload_override, message",
    [
        (None, {"input": "missing.csv"}, "does not exist"),
        ("", None, "is empty"),
        ("a,b\n1\n", None, "line 2 has 1 fields"),
        ('a\n"unclosed\n', None, "not a readable CSV"),
        (None, {"output": "no-such-dir/out.json"}, "output folder"),
        (None, {"output": "SAME"}, "different file from the input"),
        (None, {"extra": 1}, "payload must be"),
    ],
)
def test_summarize_csv_bad_input_is_permanent(tmp_path, csv_file, setup, payload_override, message):
    if setup is not None:
        csv_file.write_text(setup, encoding="utf-8")
    payload = {"input": str(csv_file), "output": str(tmp_path / "out.json")}
    for key, value in (payload_override or {}).items():
        payload[key] = str(csv_file) if value == "SAME" else (str(tmp_path / value) if isinstance(value, str) else value)
    with pytest.raises(Permanent, match=message):
        summarize_csv(payload, ctx=None)


def test_summarize_csv_rejects_non_utf8(tmp_path):
    path = tmp_path / "latin1.csv"
    path.write_bytes("city\nMontréal\n".encode("latin-1"))
    with pytest.raises(Permanent, match="not UTF-8"):
        summarize_csv({"input": str(path), "output": str(tmp_path / "o.json")}, ctx=None)


def test_summarize_csv_mixed_column_is_not_numeric(tmp_path):
    path = tmp_path / "m.csv"
    path.write_text("x,y\n1,5\nabc,6\n3,NaN\n", encoding="utf-8")
    summary = summarize_csv({"input": str(path), "output": str(tmp_path / "o.json")}, ctx=None)
    assert summary["numeric"] == {}  # x has text, y has NaN: neither is summarized


def test_summarize_csv_heartbeats_on_big_files(tmp_path, monkeypatch):
    import jobrunner.handlers as handlers

    monkeypatch.setattr(handlers, "HEARTBEAT_EVERY_ROWS", 2)
    path = tmp_path / "big.csv"
    path.write_text("n\n" + "".join(f"{i}\n" for i in range(5)), encoding="utf-8")

    class Ctx:
        beats = 0

        def heartbeat(self):
            Ctx.beats += 1

    summarize_csv({"input": str(path), "output": str(tmp_path / "o.json")}, Ctx())
    assert Ctx.beats == 2  # after rows 2 and 4
