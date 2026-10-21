import pytest
from hypothesis import given
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from jobrunner import (
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

POLICY = RetryPolicy(max_attempts=3, base_delay_ms=1_000, max_delay_ms=10_000)
HALF = lambda: 0.5  # deterministic "random" for backoff  # noqa: E731
LEASE = 30_000


def new_job(now=0, policy=POLICY):
    return submit(1, "summarize_csv", {"path": "data.csv"}, now, policy)


# --- submit and claim -----------------------------------------------------------


def test_submitted_job_is_queued_and_runnable_now():
    job = new_job(now=500)
    assert (job.state, job.attempts, job.max_attempts, job.run_after) == (State.QUEUED, 0, 3, 500)
    check_invariants(job)


def test_claim_starts_an_attempt_with_a_lease():
    job = claim(new_job(), "worker-a", now=100, lease_ms=LEASE)
    assert (job.state, job.attempts, job.token) == (State.RUNNING, 1, 1)
    assert (job.lease_owner, job.lease_expires_at) == ("worker-a", 30_100)
    check_invariants(job)


def test_a_running_job_cannot_be_claimed_again():
    job = claim(new_job(), "worker-a", 0, LEASE)
    with pytest.raises(TransitionError, match="is running, not queued"):
        claim(job, "worker-b", 1, LEASE)


def test_a_job_waiting_for_backoff_cannot_be_claimed_early():
    job = claim(new_job(), "w", 0, LEASE)
    job = fail(job, job.token, 10, "timeout", retryable=True, policy=POLICY, rand=HALF)
    assert job.run_after == 10 + 500  # half of the first ceiling (1,000 ms)
    with pytest.raises(TransitionError, match="can't run until 510"):
        claim(job, "w", 509, LEASE)
    assert claim(job, "w", 510, LEASE).attempts == 2


@pytest.mark.parametrize("kind", ["", "  ", " email", 5])
def test_invalid_kinds(kind):
    with pytest.raises(ValueError, match="kind"):
        submit(1, kind, {}, 0)


@pytest.mark.parametrize("bad", [-1, 1.5, True, None])
def test_time_must_be_whole_milliseconds(bad):
    with pytest.raises(ValueError, match="milliseconds"):
        claim(new_job(), "w", bad, LEASE)


@pytest.mark.parametrize("lease", [0, -5, 2.5])
def test_lease_must_be_positive(lease):
    with pytest.raises(ValueError, match="lease"):
        claim(new_job(), "w", 0, lease)


# --- success and failure ------------------------------------------------------------


def test_complete_records_the_result_and_releases_the_lease():
    job = claim(new_job(), "w", 0, LEASE)
    done = complete(job, job.token, 1_000, {"rows": 42})
    assert (done.state, done.result, done.lease_owner, done.lease_expires_at) == (State.SUCCEEDED, {"rows": 42}, None, None)
    check_invariants(done)


def test_retryable_failure_requeues_with_backoff():
    job = claim(new_job(), "w", 0, LEASE)
    retry = fail(job, job.token, 2_000, "connection reset", retryable=True, policy=POLICY, rand=HALF)
    assert (retry.state, retry.attempts, retry.last_error, retry.run_after) == (State.QUEUED, 1, "connection reset", 2_500)
    check_invariants(retry)


def test_permanent_failure_stops_immediately():
    job = claim(new_job(), "w", 0, LEASE)
    failed = fail(job, job.token, 10, "file not found: data.csv", retryable=False, policy=POLICY)
    assert (failed.state, failed.attempts, failed.last_error) == (State.FAILED, 1, "file not found: data.csv")
    check_invariants(failed)


def test_retries_stop_after_max_attempts():
    job, now = new_job(), 0
    for attempt in range(1, 4):
        job = claim(job, "w", now, LEASE)
        assert job.attempts == attempt
        job = fail(job, job.token, now + 1, f"timeout {attempt}", retryable=True, policy=POLICY, rand=HALF)
        now = job.run_after
    assert (job.state, job.attempts, job.last_error) == (State.FAILED, 3, "timeout 3")
    with pytest.raises(TransitionError):
        claim(job, "w", now, LEASE)


def test_long_errors_are_truncated_and_empty_ones_named():
    job = claim(new_job(), "w", 0, LEASE)
    long_error = fail(job, 1, 1, "x" * 5_000, retryable=False)
    assert len(long_error.last_error) == 2_000 and long_error.last_error.endswith("…")
    assert fail(job, 1, 1, "   ", retryable=False).last_error == "unknown error"


# --- leases and fencing -------------------------------------------------------------


def test_expired_lease_is_recovered_as_a_retry():
    job = claim(new_job(), "worker-a", 0, LEASE)
    with pytest.raises(TransitionError, match="still valid"):
        expire_lease(job, LEASE - 1, POLICY, HALF)
    recovered = expire_lease(job, LEASE, POLICY, HALF)
    assert recovered.state is State.QUEUED
    assert recovered.last_error == "lease held by worker-a expired at 30000 during attempt 1"
    check_invariants(recovered)


def test_expiring_the_last_attempt_fails_the_job():
    job = claim(submit(1, "k", None, 0, RetryPolicy(max_attempts=1)), "w", 0, LEASE)
    assert expire_lease(job, LEASE, RetryPolicy(max_attempts=1)).state is State.FAILED


def test_stale_worker_cannot_complete_after_its_lease_was_reassigned():
    # Worker A freezes; its lease expires; worker B claims the job.
    a = claim(new_job(), "worker-a", 0, LEASE)
    job = expire_lease(a, LEASE, POLICY, HALF)
    b = claim(job, "worker-b", job.run_after, LEASE)
    # Worker A wakes up and tries to report success with its old token.
    with pytest.raises(TransitionError, match="stale token 1 for job 1: attempt 2 owns it now"):
        complete(b, a.token, b.run_after + 1, {"from": "worker-a"})
    assert complete(b, b.token, b.run_after + 1, {"from": "worker-b"}).result == {"from": "worker-b"}


def test_owner_cannot_complete_after_its_own_lease_ran_out():
    # Even with the right token: once the lease is over, recovery may already have begun.
    job = claim(new_job(), "w", 0, LEASE)
    with pytest.raises(TransitionError, match="expired at 30000"):
        complete(job, job.token, LEASE, None)


def test_extending_a_lease_keeps_the_job():
    job = claim(new_job(), "w", 0, LEASE)
    job = extend_lease(job, job.token, 25_000, LEASE)
    assert job.lease_expires_at == 55_000
    with pytest.raises(TransitionError, match="still valid"):
        expire_lease(job, 40_000)
    with pytest.raises(TransitionError, match="stale token"):
        extend_lease(job, 99, 26_000, LEASE)


@pytest.mark.parametrize("terminal", ["succeeded", "failed"])
def test_terminal_jobs_never_change(terminal):
    job = claim(new_job(), "w", 0, LEASE)
    job = complete(job, 1, 1) if terminal == "succeeded" else fail(job, 1, 1, "bad input", retryable=False)
    for attempt in (
        lambda: claim(job, "w", 10**9, LEASE),
        lambda: complete(job, 1, 2),
        lambda: fail(job, 1, 2, "x", retryable=True),
        lambda: expire_lease(job, 10**9),
        lambda: extend_lease(job, 1, 2, LEASE),
    ):
        with pytest.raises(TransitionError):
            attempt()


# --- backoff --------------------------------------------------------------------------


def test_backoff_ceiling_doubles_up_to_the_cap():
    top = lambda: 0.999999  # noqa: E731
    policy = RetryPolicy(max_attempts=10, base_delay_ms=1_000, max_delay_ms=10_000)
    assert [policy.backoff(n, top) for n in range(1, 7)] == [999, 1_999, 3_999, 7_999, 9_999, 9_999]


def test_backoff_never_overflows_on_huge_attempt_numbers():
    assert RetryPolicy().backoff(10_000, lambda: 0.5) == 30_000


@given(st.integers(1, 200), st.floats(0, 1, exclude_max=True))
def test_backoff_is_between_zero_and_the_cap(attempt, fraction):
    assert 0 <= POLICY.backoff(attempt, lambda: fraction) <= POLICY.max_delay_ms


@pytest.mark.parametrize("bad", [1.0, -0.1, 2])
def test_backoff_rejects_a_bad_random_source(bad):
    with pytest.raises(ValueError, match=r"\[0, 1\)"):
        POLICY.backoff(1, lambda: bad)


@pytest.mark.parametrize(
    "kwargs", [{"max_attempts": 0}, {"base_delay_ms": -1}, {"base_delay_ms": 10, "max_delay_ms": 5}, {"max_attempts": 2.5}]
)
def test_invalid_policies(kwargs):
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)


def test_jobs_are_immutable():
    job = new_job()
    with pytest.raises(AttributeError):
        job.state = State.SUCCEEDED


# --- a hostile scheduler (Hypothesis stateful testing) --------------------------------


class JobLifecycle(RuleBasedStateMachine):
    """Several workers claim, stall, retry, and report late with old tokens, in random order.

    After every step the job must satisfy the invariants, and any rejected request must
    leave it exactly as it was.
    """

    def __init__(self):
        super().__init__()
        self.now = 0
        self.job = submit(1, "k", None, 0, POLICY)
        self.tokens_seen: list[int] = []  # every token ever handed out, including stale ones
        self.finished = None

    def attempt(self, action):
        before = self.job
        try:
            self.job = action()
        except TransitionError:
            assert self.job == before  # a rejected request changes nothing

    # Mostly short steps, plus jumps past the lease so expiry and stale tokens happen often.
    @rule(ms=st.one_of(st.integers(0, 2_000), st.integers(LEASE, LEASE + 15_000)))
    def time_passes(self, ms):
        self.now += ms

    @rule(worker=st.sampled_from(["a", "b", "c"]))
    def try_claim(self, worker):
        before = self.job.attempts
        self.attempt(lambda: claim(self.job, worker, self.now, LEASE))
        if self.job.attempts != before:
            assert self.job.attempts == before + 1
            self.tokens_seen.append(self.job.token)

    @precondition(lambda self: self.tokens_seen)
    @rule(data=st.data(), result=st.integers())
    def try_complete(self, data, result):
        token = data.draw(st.sampled_from(self.tokens_seen))
        if token != self.job.attempts and self.job.state is State.RUNNING:
            with pytest.raises(TransitionError, match="stale token"):
                complete(self.job, token, self.now, result)
        self.attempt(lambda: complete(self.job, token, self.now, result))

    @precondition(lambda self: self.tokens_seen)
    @rule(data=st.data(), retryable=st.booleans())
    def try_fail(self, data, retryable):
        token = data.draw(st.sampled_from(self.tokens_seen))
        self.attempt(lambda: fail(self.job, token, self.now, "boom", retryable=retryable, policy=POLICY, rand=HALF))

    @rule()
    def try_expire(self):
        self.attempt(lambda: expire_lease(self.job, self.now, POLICY, HALF))

    @precondition(lambda self: self.tokens_seen)
    @rule(data=st.data())
    def try_extend(self, data):
        token = data.draw(st.sampled_from(self.tokens_seen))
        self.attempt(lambda: extend_lease(self.job, token, self.now, LEASE))

    @invariant()
    def lifecycle_rules_hold(self):
        check_invariants(self.job)

    @invariant()
    def terminal_states_are_final(self):
        if self.finished is not None:
            assert self.job == self.finished
        elif self.job.state.terminal:
            self.finished = self.job

    @invariant()
    def attempts_match_claims(self):
        assert self.job.attempts == len(self.tokens_seen)


TestJobLifecycle = JobLifecycle.TestCase
