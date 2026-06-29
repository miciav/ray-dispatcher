# Phase 6a — Ray-Free Job Orchestration (`run_job` + retry + result) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the deterministic, Ray-free per-job orchestration in `scheduling.py`: retry classification (`should_retry`, §8.3), `JobResult` assembly (`assemble_job_result`, §4.4), and the `run_job` retry loop that acquires a lease, runs `execute_attempt`, classifies failures, retries on a *different* host, and returns a `JobResult`.

**Architecture:** `run_job` is the body the Phase 6b Ray task will call inside `ray.get`. It is fully synchronous and deterministic — the lease is injected as a small `LeaseHandle` protocol (the real async Ray actor is adapted to it in 6b; tests inject a fake). Failures from `execute_attempt` (command outcome) and exceptions it raises (transport) are classified into `AttemptResult`s; `should_retry` decides whether to try another host; `assemble_job_result` folds the attempts into the final `JobResult`. No Ray, no threads, no timeout here.

**Tech Stack:** Python 3.10+, stdlib `typing.Protocol`; existing `ray_dispatcher.scheduling` (`execute_attempt`, `HostRuntime`, `Lease`), `ray_dispatcher.results.JobLayout`, `ray_dispatcher.ssh` (`Transport`, `TransportError`), `ray_dispatcher.models`. Tests use `pytest` + `tmp_path` + `FakeTransport` + a fake lease.

## Global Constraints

- Python floor 3.10; `from __future__ import annotations` at the top of every module.
- mypy strict (`files=["src"]`) and ruff (`E/F/I/UP/B`, line-length 100) must pass with zero errors. Stdlib-only.
- `scheduling.py` must remain Ray-free in this phase (the `ray.remote` decoration is Phase 6b). It must NOT import `ray`.
- Data types already in `models.py` (do NOT redefine): `Job`, `RetryPolicy`, `JobResult`, `AttemptResult`, `JobStatus`, `FailureKind`. `RetryPolicy(max_attempts=2, retry_on=frozenset({FailureKind.SSH, FailureKind.HOST_LOST, FailureKind.COLLECTION}))`; `RetryPolicy.max_attempts >= 1`.
- §8.3 retry policy: retry SSH / HOST_LOST / COLLECTION failures up to `max_attempts` total attempts. COMMAND, OUTPUT_MISSING, TIMEOUT are NOT retried by default (deterministic). A retry passes previously attempted hosts as exclusions; a host is reused only after every healthy host has been tried (the `LeasePool.acquire` exclusion logic already implements the reuse rule — `run_job` just passes the tried-host set).
- §4.4: `JobResult.returncode` and `host` describe the FINAL attempt; attempt-level details remain in `attempts`. Result durations are monotonic elapsed time.
- `execute_attempt` (Phase 5b) classifies the command outcome (`COMMAND` / `OUTPUT_MISSING` / success) and propagates transport/setup failures as exceptions (`TransportError` / `DispatcherError`) for this orchestration layer to classify (`SSH` / `INTERNAL`).

**Deferred to Phase 6b (do NOT build here):** `ray.remote` decoration; the Ray task; the concurrent lease heartbeat while the job runs (§8.2); timeout enforcement + process termination (§8.1, `TIMEOUT`); `SshRayBackend`; `Dispatcher`; status registry (§9.2); §3.2.6 stale-lock reconciliation; `result.json` writing (the backend writes it after `run_job`); public exports.

---

### Task 1: `should_retry` (§8.3 retry classification)

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (add `should_retry`; extend imports)
- Test: `tests/unit/test_should_retry.py`

**Interfaces:**
- Consumes: `models.RetryPolicy`, `models.FailureKind`.
- Produces: `should_retry(policy: RetryPolicy, kind: FailureKind | None, completed_attempts: int) -> bool` — True iff `kind` is a retryable failure (`kind in policy.retry_on`) AND `completed_attempts < policy.max_attempts`. Success (`kind is None`) is never retried.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_should_retry.py`:

```python
from ray_dispatcher.models import FailureKind, RetryPolicy
from ray_dispatcher.scheduling import should_retry


def test_retryable_kind_under_budget_retries():
    p = RetryPolicy()  # max_attempts=2, retry_on={SSH, HOST_LOST, COLLECTION}
    assert should_retry(p, FailureKind.SSH, completed_attempts=1) is True


def test_retryable_kind_at_budget_stops():
    p = RetryPolicy()
    assert should_retry(p, FailureKind.SSH, completed_attempts=2) is False  # used the 2 allowed


def test_non_retryable_kinds_never_retry():
    p = RetryPolicy()
    assert should_retry(p, FailureKind.COMMAND, completed_attempts=1) is False
    assert should_retry(p, FailureKind.OUTPUT_MISSING, completed_attempts=1) is False
    assert should_retry(p, FailureKind.TIMEOUT, completed_attempts=1) is False


def test_success_never_retries():
    p = RetryPolicy()
    assert should_retry(p, None, completed_attempts=1) is False


def test_opt_in_retry_on_command():
    p = RetryPolicy(retry_on=frozenset({FailureKind.COMMAND}))
    assert should_retry(p, FailureKind.COMMAND, completed_attempts=1) is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_should_retry.py -v`
Expected: FAIL with `ImportError: cannot import name 'should_retry'`.

- [ ] **Step 3: Write the implementation**

Extend the `models` import in `scheduling.py` to include `RetryPolicy` and `JobResult` (used in Task 2/3 too): change the line to
`from .models import AttemptResult, FailureKind, Job, JobResult, JobStatus, Project, RetryPolicy`.

Append `should_retry` at module level after `execute_attempt` (before `class LeaseService`):

```python
def should_retry(policy: RetryPolicy, kind: FailureKind | None, completed_attempts: int) -> bool:
    """Decide whether to make another attempt after a failure (spec §8.3).

    Retries only a configured-retryable failure kind, and only while attempts
    remain. Success and non-retryable kinds (COMMAND/OUTPUT_MISSING/TIMEOUT by
    default) stop immediately.
    """
    if kind is None or kind not in policy.retry_on:
        return False
    return completed_attempts < policy.max_attempts
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_should_retry.py -v`
Expected: all five cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/scheduling.py tests/unit/test_should_retry.py
git commit -m "feat: add should_retry retry classification (§8.3)"
```

---

### Task 2: `assemble_job_result` (§4.4 JobResult assembly)

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (add `assemble_job_result`)
- Test: `tests/unit/test_assemble_job_result.py`

**Interfaces:**
- Consumes: `models.AttemptResult`, `models.JobResult`, `models.JobStatus`.
- Produces: `assemble_job_result(job_id: str, batch_id: str, attempts: list[AttemptResult], *, outputs_dir: str) -> JobResult` — the final attempt drives `status`/`returncode`/`host`/`error`; `duration_s` is the sum of attempt durations (total work time); `output_dir` is `outputs_dir` only when the final attempt SUCCEEDED, else `None`; `attempts` is the full tuple. Requires at least one attempt.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_assemble_job_result.py`:

```python
from ray_dispatcher.models import AttemptResult, FailureKind, JobStatus
from ray_dispatcher.scheduling import assemble_job_result


def _attempt(n, host, status, rc, dur, fk=None, err=None):
    return AttemptResult(
        number=n, host=host, status=status, returncode=rc, duration_s=dur,
        stdout_log=f"a/{n}/stdout.log", stderr_log=f"a/{n}/stderr.log",
        failure_kind=fk, error=err,
    )


def test_success_uses_final_attempt_and_sets_output_dir():
    attempts = [
        _attempt(1, "a", JobStatus.FAILED, None, 1.0, FailureKind.SSH, "ssh down"),
        _attempt(2, "b", JobStatus.SUCCEEDED, 0, 2.5),
    ]
    r = assemble_job_result("jobA", "b1", attempts, outputs_dir="/res/b1/jobA/outputs")
    assert r.status is JobStatus.SUCCEEDED
    assert r.host == "b"               # final attempt
    assert r.returncode == 0
    assert r.duration_s == 3.5         # sum across attempts
    assert r.output_dir == "/res/b1/jobA/outputs"
    assert len(r.attempts) == 2
    assert r.error is None


def test_failure_has_no_output_dir_and_keeps_final_error():
    attempts = [_attempt(1, "a", JobStatus.FAILED, 3, 1.0, FailureKind.COMMAND, "boom")]
    r = assemble_job_result("jobA", "b1", attempts, outputs_dir="/res/b1/jobA/outputs")
    assert r.status is JobStatus.FAILED
    assert r.host == "a"
    assert r.returncode == 3
    assert r.output_dir is None        # no publish on failure
    assert r.error == "boom"
    assert r.id == "jobA" and r.batch_id == "b1"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_assemble_job_result.py -v`
Expected: FAIL with `ImportError: cannot import name 'assemble_job_result'`.

- [ ] **Step 3: Write the implementation**

Append `assemble_job_result` after `should_retry`:

```python
def assemble_job_result(
    job_id: str, batch_id: str, attempts: list[AttemptResult], *, outputs_dir: str
) -> JobResult:
    """Fold the attempt history into the job's final result (spec §4.4).

    The final attempt describes returncode/host/error; duration is the total
    across attempts; output_dir is set only when the final attempt succeeded.
    """
    final = attempts[-1]
    return JobResult(
        id=job_id,
        batch_id=batch_id,
        status=final.status,
        returncode=final.returncode,
        duration_s=sum(a.duration_s for a in attempts),
        host=final.host,
        output_dir=outputs_dir if final.status is JobStatus.SUCCEEDED else None,
        attempts=tuple(attempts),
        error=final.error,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_assemble_job_result.py -v`
Expected: both cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/scheduling.py tests/unit/test_assemble_job_result.py
git commit -m "feat: add assemble_job_result (§4.4)"
```

---

### Task 3: `LeaseHandle` protocol + `run_job` retry loop

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (add `LeaseHandle`, `_failed_attempt`, `run_job`; extend imports)
- Test: `tests/unit/test_run_job.py`

**Interfaces:**
- Consumes: `should_retry`, `assemble_job_result`, `execute_attempt`, `HostRuntime`, `Lease` (all in `scheduling.py`); `results.JobLayout`; `ssh.Transport`/`TransportError`; `models.Job`/`RetryPolicy`/`AttemptResult`/`JobResult`/`JobStatus`/`FailureKind`; stdlib `typing.Protocol`, `collections.abc.Callable`/`Iterable`.
- Produces:
  - `LeaseHandle` Protocol — `acquire(self, attempt_id: str, *, exclude: Iterable[str] = ()) -> Lease` and `release(self, token: str) -> None`. (The Phase 6b Ray-actor adapter and the test fake both satisfy it. Heartbeat is omitted here — added in 6b.)
  - `run_job(job: Job, *, batch_id: str, lease: LeaseHandle, runtime_for: Callable[[str], HostRuntime], transport_for: Callable[[str], Transport], local: JobLayout, policy: RetryPolicy) -> JobResult` — the retry loop: per attempt, acquire a lease excluding already-tried hosts, run `execute_attempt`, classify any raised `TransportError` as an `SSH` failure (and `DispatcherError` as `INTERNAL`), release the lease, and retry on a different host while `should_retry` holds; then `assemble_job_result`. Propagates `NoHealthyHostsError` from `acquire` (no healthy capacity, §8.2).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_run_job.py`:

```python
import json

from ray_dispatcher.models import FailureKind, Job, JobStatus, RetryPolicy
from ray_dispatcher.provisioning import RemoteLayout
from ray_dispatcher.results import JobLayout
from ray_dispatcher.scheduling import HostRuntime, Lease, run_job
from ray_dispatcher.ssh import CommandResult, FakeTransport, TransportError


class FakeLease:
    """Hands out hosts in order; records the exclude set seen on each acquire."""

    def __init__(self, hosts):
        self._hosts = list(hosts)
        self._n = 0
        self.excludes = []

    def acquire(self, attempt_id, *, exclude=()):
        self.excludes.append(set(exclude))
        host = self._hosts[self._n]
        self._n += 1
        return Lease(token=f"tok{self._n}", host=host, slot=0,
                     attempt_id=attempt_id, expiry_s=0.0, heartbeat_s=0.0)

    def release(self, token):
        pass


def _runtime(host):
    return HostRuntime(host=host, layout=RemoteLayout("/home/u", "dfaas"),
                       environment_digest="env1", runner_digest="run1",
                       project_path="/proj", secret_env={})


def _ok_transport(rc=0):
    def results(argv):
        if argv[0] == "cat" and argv[1].endswith("result.json"):
            return CommandResult(0, json.dumps(
                {"returncode": rc, "started_at": 1.0, "ended_at": 2.0, "duration_s": 1.5}), "", 0.0)
        return CommandResult(0, "", "", 0.0)
    return FakeTransport(run_results=results)


def _ssh_failing_transport():
    def results(argv):
        if argv[0] == "python3":             # the runner invocation
            raise TransportError("ssh dropped")
        return CommandResult(0, "", "", 0.0)
    return FakeTransport(run_results=results)


def _layout(tmp_path, job_id="jobA"):
    return JobLayout(str(tmp_path / "results"), "b1", job_id)


def test_success_first_attempt(tmp_path):
    lease = FakeLease(["a"])
    job = Job(id="jobA", command=("python", "run.py"))  # no outputs
    r = run_job(job, batch_id="b1", lease=lease,
                runtime_for=_runtime, transport_for=lambda h: _ok_transport(0),
                local=_layout(tmp_path), policy=RetryPolicy())
    assert r.status is JobStatus.SUCCEEDED
    assert r.host == "a"
    assert len(r.attempts) == 1
    assert lease.excludes == [set()]        # first acquire excludes nothing


def test_ssh_failure_retries_on_different_host(tmp_path):
    lease = FakeLease(["a", "b"])
    transports = {"a": _ssh_failing_transport(), "b": _ok_transport(0)}
    job = Job(id="jobA", command=("python", "run.py"))
    r = run_job(job, batch_id="b1", lease=lease,
                runtime_for=_runtime, transport_for=lambda h: transports[h],
                local=_layout(tmp_path), policy=RetryPolicy())
    assert r.status is JobStatus.SUCCEEDED
    assert r.host == "b"                     # retried elsewhere
    assert len(r.attempts) == 2
    assert r.attempts[0].failure_kind is FailureKind.SSH
    assert lease.excludes == [set(), {"a"}]  # second acquire excludes the tried host


def test_ssh_failure_exhausts_attempts(tmp_path):
    lease = FakeLease(["a", "b"])
    job = Job(id="jobA", command=("python", "run.py"))
    r = run_job(job, batch_id="b1", lease=lease,
                runtime_for=_runtime, transport_for=lambda h: _ssh_failing_transport(),
                local=_layout(tmp_path), policy=RetryPolicy())  # max_attempts=2
    assert r.status is JobStatus.FAILED
    assert r.attempts[-1].failure_kind is FailureKind.SSH
    assert len(r.attempts) == 2              # stopped at the budget


def test_command_failure_is_not_retried(tmp_path):
    lease = FakeLease(["a", "b"])
    job = Job(id="jobA", command=("python", "run.py"))
    r = run_job(job, batch_id="b1", lease=lease,
                runtime_for=_runtime, transport_for=lambda h: _ok_transport(rc=3),
                local=_layout(tmp_path), policy=RetryPolicy())
    assert r.status is JobStatus.FAILED
    assert r.returncode == 3
    assert r.attempts[-1].failure_kind is FailureKind.COMMAND
    assert len(r.attempts) == 1              # COMMAND not retried
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_run_job.py -v`
Expected: FAIL with `ImportError: cannot import name 'run_job'`.

- [ ] **Step 3: Write the implementation**

Add `from typing import Protocol` to `scheduling.py`'s imports (ruff orders stdlib `from` imports by module, so it goes immediately after `from dataclasses import dataclass, replace`; if a `from typing import` line already exists, merge into it). Ensure `TransportError` is imported from `.ssh`: change the ssh import line to
`from .ssh import CommandResult, Transport, TransportError, terminate_process_group, write_remote_file`.

Append after `assemble_job_result`:

```python
class LeaseHandle(Protocol):
    """The slice of the lease actor that run_job needs (6b adapts the Ray actor)."""

    def acquire(self, attempt_id: str, *, exclude: Iterable[str] = ()) -> Lease: ...

    def release(self, token: str) -> None: ...


def _failed_attempt(
    n: int, host: str, kind: FailureKind, error: str, local: JobLayout
) -> AttemptResult:
    """Build the AttemptResult for an attempt that raised before producing one."""
    return AttemptResult(
        number=n,
        host=host,
        status=JobStatus.FAILED,
        returncode=None,
        duration_s=0.0,
        stdout_log=str(local.stdout_log(n)),
        stderr_log=str(local.stderr_log(n)),
        failure_kind=kind,
        error=error,
    )


def run_job(
    job: Job,
    *,
    batch_id: str,
    lease: LeaseHandle,
    runtime_for: Callable[[str], HostRuntime],
    transport_for: Callable[[str], Transport],
    local: JobLayout,
    policy: RetryPolicy,
) -> JobResult:
    """Run one logical job to a terminal JobResult, retrying per policy (spec §8.3).

    Each attempt leases a host (excluding already-tried hosts so retries prefer a
    fresh VM, §7.1), runs execute_attempt, and classifies the outcome. A raised
    TransportError becomes an SSH failure and a DispatcherError an INTERNAL
    failure (both surfaced as AttemptResults). NoHealthyHostsError from acquire
    propagates. The Phase 6b Ray task wraps this with heartbeat + timeout.
    """
    attempts: list[AttemptResult] = []
    tried: set[str] = set()
    while True:
        n = len(attempts) + 1
        leased = lease.acquire(job.id, exclude=tried)
        try:
            result = execute_attempt(
                transport_for(leased.host), runtime_for(leased.host), job,
                batch_id=batch_id, attempt=n, local=local,
            )
        except TransportError as e:
            result = _failed_attempt(n, leased.host, FailureKind.SSH, str(e), local)
        except DispatcherError as e:
            result = _failed_attempt(n, leased.host, FailureKind.INTERNAL, str(e), local)
        finally:
            lease.release(leased.token)
        attempts.append(result)
        tried.add(leased.host)
        if result.status is JobStatus.SUCCEEDED or not should_retry(
            policy, result.failure_kind, len(attempts)
        ):
            break
    return assemble_job_result(
        job.id, batch_id, attempts, outputs_dir=str(local.outputs_dir)
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_run_job.py -v`
Expected: all four cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/scheduling.py tests/unit/test_run_job.py
git commit -m "feat: add run_job retry loop with different-host preference (§8.3)"
```

---

### Task 4: Phase 6a gate

**Files:**
- (no new source) — final full-toolchain verification.

**Interfaces:** none.

- [ ] **Step 1: Run the full suite**

Run: `uv run pytest -q`
Expected: all tests `passed` (Phases 1–5b + Phase 6a).

- [ ] **Step 2: Lint + type check**

Run: `uv run ruff check --fix . && uv run ruff check . && uv run mypy`
Expected: ruff auto-sorts then `All checks passed!`; mypy `Success: no issues found`.

If mypy flags the `LeaseHandle` Protocol or the `Callable` injections, fix minimally (no config relaxation). Confirm `scheduling.py` still does NOT import `ray`.

- [ ] **Step 3: Commit (only if Step 2 made changes)**

```bash
git add -A
git commit -m "chore: phase 6a gate green (ruff + mypy)"
```

---

## Phase 6a self-review

Run before declaring Phase 6a done:

- [ ] `should_retry` retries only `kind in policy.retry_on` while `completed_attempts < max_attempts`; success/COMMAND/OUTPUT_MISSING/TIMEOUT never retry by default; opt-in via `retry_on` works (§8.3).
- [ ] `assemble_job_result` takes returncode/host/error/status from the FINAL attempt, sums durations, sets `output_dir` only on success, keeps the full `attempts` tuple (§4.4).
- [ ] `run_job` acquires excluding the tried-host set (different-host preference, §7.1), classifies `TransportError`→`SSH` and `DispatcherError`→`INTERNAL`, always releases the lease (finally), stops on success or when `should_retry` is false, and propagates `NoHealthyHostsError`.
- [ ] `LeaseHandle` is a minimal Protocol (`acquire`/`release`) the 6b actor adapter and the test fake both satisfy.
- [ ] `uv run pytest -q`, `uv run ruff check .`, `uv run mypy` all green; `scheduling.py` does not import `ray`.

**Deliverable:** `should_retry`, `assemble_job_result`, `LeaseHandle`, `_failed_attempt`, `run_job` in `scheduling.py` — the deterministic per-job orchestration the Phase 6b Ray task wraps.

**Residuals carried to Phase 6b (documented):**
- **Ray task + actor:** `HostLease = ray.remote(num_cpus=0)(LeaseService)`; a Ray task (`num_cpus=0`, `resources={"vm_slot": 1}`, `max_retries=0`) adapts the async actor to `LeaseHandle` (via `ray.get`) and calls `run_job`; the backend writes `result.json` via `results.write_result_json` after `run_job` returns.
- **Heartbeat (§8.2):** the Ray task beats the lease concurrently while `run_job`'s `execute_attempt` blocks (run_job itself does not heartbeat).
- **Timeout + termination (§8.1):** enforce `job.timeout_s` on the runner invoke and terminate the recorded process group on timeout, classifying `TIMEOUT` — added to `execute_attempt`/the task in 6b alongside real transport timeout semantics.
- **Backend/Dispatcher (§3.2/§3.3/§4.5):** `SshRayBackend` (exclusive runtime, `vm_slot` resource, provisioning, submit/status/cancel/resolve/teardown), `Dispatcher` (batch orchestration, `BatchExistsError`, `raise_on_failure`→`BatchFailedError`, result ordering), status registry (§9.2), §3.2.6 stale-lock reconciliation, and the local-Ray integration tests (§11).
