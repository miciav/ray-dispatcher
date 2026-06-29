# Phase 6d — Dispatcher lifecycle + batch orchestration + public exports

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create `src/ray_dispatcher/dispatcher.py` with the public `Dispatcher` class (§4.5): full lifecycle (`setup`, `submit`, `status`, `cancel`, `as_completed`, `run`, `teardown`, context manager) + `BatchExistsError`/`BatchFailedError` raise semantics + `require_all_hosts` guard + `__init__.py` exports for `Dispatcher` and `SshRayBackend`.

**Architecture:** `Dispatcher` is a thin orchestration layer over `ExecutionBackend`: it owns setup/teardown sequencing, batch-dir collision detection, auto-setup on first submit, result ordering, `raise_on_failure`, and cleanup-error isolation in `__exit__`. It does NOT import ray directly — all Ray interaction goes through the backend. `SshRayBackend` is the default backend when none is provided.

**Tech Stack:** Python 3.10+ stdlib only (`uuid`, `time`, `logging`, `pathlib`). Existing: `ExecutionBackend` (`backends/base.py`), `SshRayBackend` (`backends/ssh_ray.py`), all model types (`models.py`), `BatchExistsError`/`BatchFailedError`/`ProvisioningError`/`DispatcherError` (`errors.py`).

## Global Constraints

- Python floor 3.10; `from __future__ import annotations` at top of every modified/created source module (test files follow repo convention — omit it)
- mypy strict (`files=["src"]`) and ruff (`E/F/I/UP/B`, line-length 100) must pass with zero errors
- `dispatcher.py` must NOT import ray — all Ray interaction goes through the backend interface
- `BatchExistsError` and `BatchFailedError` already exist in `errors.py` — do NOT redefine them
- `require_all_hosts=True`: if any host failed provisioning, raise `ProvisioningError(report)` after `backend.setup()` returns
- `setup(force=True)` after setup is already done: raise `DispatcherError("setup(force=True) is rejected after Ray has started")`
- `submit()` auto-calls `setup()` if not yet done (auto-setup path)
- `submit()`: if `Path(results_dir) / batch_id` already exists, raise `BatchExistsError`
- `as_completed(handles) -> Iterator[JobResult]` yields in completion order; poll `backend.status` every 0.1 s
- `run(jobs, *, batch_id=None) -> list[JobResult]`: submit + drain via `as_completed` + return in **input order** + raise `BatchFailedError(ordered_results)` if `raise_on_failure=True` and any result is not SUCCEEDED
- `teardown(*, purge=False)`: cancel all outstanding handles, delegate `backend.teardown(purge=purge)`; if `purge=True` and any handle is still PENDING/RUNNING, raise `DispatcherError`
- `__enter__` returns self (no network operation); `__exit__` calls `teardown()`, swallows cleanup errors if an exception is already in flight (log them with `logging.exception`), re-raises if no prior exception
- `__init__.py`: add `Dispatcher` and `SshRayBackend` to imports and `__all__`

**Deferred (do NOT build here):** Phase 7 Multipass e2e tests; §9.2 Rich progress rendering (`progress` extra); `setup(force=True)` re-provisioning logic; stale-lock reconciliation at provisioning takeover (§3.2.6); purge remote state.

---

### Task 1: `dispatcher.py` — Dispatcher class

**Files:**
- Create: `src/ray_dispatcher/dispatcher.py`
- Test: `tests/unit/test_dispatcher.py`

**Interfaces:**
- Consumes: `ExecutionBackend` from `backends.base`; `SshRayBackend` from `backends.ssh_ray`; `Inventory`, `Project`, `Job`, `JobHandle`, `JobResult`, `JobStatus`, `RetryPolicy`, `ProvisioningReport` from `models`; `BatchExistsError`, `BatchFailedError`, `DispatcherError`, `ProvisioningError` from `errors`.
- Produces: `Dispatcher` class with full §4.5 interface — see exact signatures below.

**Exact `Dispatcher` interface (use verbatim):**

```python
class Dispatcher:
    def __init__(
        self,
        inventory: Inventory,
        project: Project,
        *,
        results_dir: str = "./results",
        retry_policy: RetryPolicy = RetryPolicy(),  # noqa: B008
        raise_on_failure: bool = False,
        require_all_hosts: bool = True,
        backend: ExecutionBackend | None = None,
    ) -> None: ...

    def setup(self, *, force: bool = False) -> ProvisioningReport: ...
    def submit(self, jobs: Sequence[Job], *, batch_id: str | None = None) -> list[JobHandle]: ...
    def status(self, handle: JobHandle) -> JobStatus: ...
    def cancel(self, handle: JobHandle) -> None: ...
    def as_completed(self, handles: Sequence[JobHandle]) -> Iterator[JobResult]: ...
    def run(self, jobs: Sequence[Job], *, batch_id: str | None = None) -> list[JobResult]: ...
    def teardown(self, *, purge: bool = False) -> None: ...
    def __enter__(self) -> "Dispatcher": ...
    def __exit__(self, *exc: object) -> None: ...
```

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_dispatcher.py`:

```python
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ray_dispatcher.backends.base import ExecutionBackend
from ray_dispatcher.dispatcher import Dispatcher
from ray_dispatcher.errors import BatchExistsError, BatchFailedError, DispatcherError, ProvisioningError
from ray_dispatcher.models import (
    HostProvisioningResult,
    Inventory,
    Job,
    JobHandle,
    JobResult,
    JobStatus,
    Project,
    ProvisioningReport,
    RemoteHost,
    RetryPolicy,
)


def _inv():
    return Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=1),))


def _proj():
    return Project(path="/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25")


def _report(succeeded: bool = True):
    return ProvisioningReport((HostProvisioningResult("10.0.0.1", succeeded, "src123", "env123"),))


def _mock_backend(report=None):
    b = MagicMock(spec=ExecutionBackend)
    b.setup.return_value = report or _report()
    b.submit.side_effect = lambda batch_id, job: JobHandle(
        batch_id=batch_id, job_id=job.id, token=uuid.uuid4().hex
    )
    b.status.return_value = JobStatus.SUCCEEDED
    b.resolve.side_effect = lambda h: JobResult(
        id=h.job_id, batch_id=h.batch_id, status=JobStatus.SUCCEEDED,
        returncode=0, duration_s=0.1, host="10.0.0.1", output_dir=None, attempts=(),
    )
    return b


def test_setup_calls_backend_setup():
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b)
    report = d.setup()
    b.setup.assert_called_once()
    assert report.hosts[0].succeeded


def test_setup_idempotent_returns_same_report():
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b)
    r1 = d.setup()
    r2 = d.setup()
    assert r1 is r2
    b.setup.assert_called_once()  # only called once


def test_setup_force_after_setup_raises():
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b)
    d.setup()
    with pytest.raises(DispatcherError, match="force"):
        d.setup(force=True)


def test_require_all_hosts_raises_on_partial_failure():
    b = _mock_backend(report=ProvisioningReport((
        HostProvisioningResult("10.0.0.1", True, "src123", "env123"),
        HostProvisioningResult("10.0.0.2", False, None, None),
    )))
    d = Dispatcher(_inv(), _proj(), backend=b, require_all_hosts=True)
    with pytest.raises(ProvisioningError):
        d.setup()


def test_require_all_hosts_false_allows_partial():
    b = _mock_backend(report=ProvisioningReport((
        HostProvisioningResult("10.0.0.1", True, "src123", "env123"),
        HostProvisioningResult("10.0.0.2", False, None, None),
    )))
    d = Dispatcher(_inv(), _proj(), backend=b, require_all_hosts=False)
    report = d.setup()
    assert report is not None


def test_submit_auto_sets_up(tmp_path):
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b, results_dir=str(tmp_path))
    job = Job(id="j1", command=("echo",))
    handles = d.submit([job])
    b.setup.assert_called_once()
    assert len(handles) == 1
    assert handles[0].job_id == "j1"


def test_submit_generates_batch_id_when_none(tmp_path):
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b, results_dir=str(tmp_path))
    handles = d.submit([Job(id="j1", command=("echo",))])
    assert handles[0].batch_id  # non-empty


def test_submit_raises_batch_exists_error(tmp_path):
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b, results_dir=str(tmp_path))
    batch_id = "mybatch"
    (tmp_path / batch_id).mkdir()
    with pytest.raises(BatchExistsError):
        d.submit([Job(id="j1", command=("echo",))], batch_id=batch_id)


def test_status_delegates_to_backend():
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b)
    d.setup()
    h = JobHandle(batch_id="b1", job_id="j1", token="tok1")
    s = d.status(h)
    b.status.assert_called_once_with(h)
    assert s == JobStatus.SUCCEEDED


def test_cancel_delegates_to_backend():
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b)
    d.setup()
    h = JobHandle(batch_id="b1", job_id="j1", token="tok1")
    d.cancel(h)
    b.cancel.assert_called_once_with(h)


def test_as_completed_yields_in_completion_order(tmp_path):
    b = _mock_backend()
    # Make status return RUNNING first, then SUCCEEDED on second call per handle
    call_counts: dict[str, int] = {}
    def _status(h):
        call_counts.setdefault(h.token, 0)
        call_counts[h.token] += 1
        return JobStatus.RUNNING if call_counts[h.token] < 2 else JobStatus.SUCCEEDED
    b.status.side_effect = _status

    d = Dispatcher(_inv(), _proj(), backend=b, results_dir=str(tmp_path))
    jobs = [Job(id=f"j{i}", command=("echo",)) for i in range(3)]
    handles = d.submit(jobs)
    results = list(d.as_completed(handles))
    assert len(results) == 3
    for r in results:
        assert r.status == JobStatus.SUCCEEDED


def test_run_returns_results_in_input_order(tmp_path):
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b, results_dir=str(tmp_path))
    jobs = [Job(id=f"job-{i}", command=("echo",)) for i in range(5)]
    results = d.run(jobs)
    assert [r.id for r in results] == [j.id for j in jobs]


def test_run_raises_batch_failed_error(tmp_path):
    b = _mock_backend()
    b.resolve.side_effect = lambda h: JobResult(
        id=h.job_id, batch_id=h.batch_id, status=JobStatus.FAILED,
        returncode=1, duration_s=0.0, host=None, output_dir=None, attempts=(),
    )
    d = Dispatcher(_inv(), _proj(), backend=b, raise_on_failure=True, results_dir=str(tmp_path))
    with pytest.raises(BatchFailedError) as exc:
        d.run([Job(id="j1", command=("fail",))])
    assert len(exc.value.results) == 1


def test_run_drains_all_before_raising(tmp_path):
    """raise_on_failure=True still drains all jobs before raising."""
    b = _mock_backend()
    resolved: list[str] = []
    def _resolve(h):
        resolved.append(h.job_id)
        return JobResult(id=h.job_id, batch_id=h.batch_id, status=JobStatus.FAILED,
                         returncode=1, duration_s=0.0, host=None, output_dir=None, attempts=())
    b.resolve.side_effect = _resolve
    d = Dispatcher(_inv(), _proj(), backend=b, raise_on_failure=True, results_dir=str(tmp_path))
    with pytest.raises(BatchFailedError):
        d.run([Job(id=f"j{i}", command=("fail",)) for i in range(3)])
    assert len(resolved) == 3


def test_teardown_cancels_outstanding_and_delegates(tmp_path):
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b, results_dir=str(tmp_path))
    handles = d.submit([Job(id="j1", command=("echo",))])
    d.teardown()
    b.cancel.assert_called()
    b.teardown.assert_called_once_with(purge=False)


def test_teardown_purge_rejected_when_active(tmp_path):
    b = _mock_backend()
    b.status.return_value = JobStatus.RUNNING
    d = Dispatcher(_inv(), _proj(), backend=b, results_dir=str(tmp_path))
    d.submit([Job(id="j1", command=("echo",))])
    with pytest.raises(DispatcherError, match="purge"):
        d.teardown(purge=True)


def test_context_manager_calls_teardown():
    b = _mock_backend()
    with Dispatcher(_inv(), _proj(), backend=b) as d:
        assert d is not None
    b.teardown.assert_called_once()


def test_context_manager_enter_does_no_network():
    b = _mock_backend()
    with Dispatcher(_inv(), _proj(), backend=b):
        pass
    b.setup.assert_not_called()  # __enter__ must not call setup


def test_context_manager_swallows_cleanup_error_when_exception_in_flight():
    b = _mock_backend()
    b.teardown.side_effect = RuntimeError("cleanup boom")
    with pytest.raises(ValueError, match="original"):
        with Dispatcher(_inv(), _proj(), backend=b):
            raise ValueError("original")
    # RuntimeError from teardown must NOT replace the ValueError
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_dispatcher.py -v`
Expected: FAIL — `cannot import name 'Dispatcher' from 'ray_dispatcher.dispatcher'` (module does not exist yet).

- [ ] **Step 3: Write the implementation**

Create `src/ray_dispatcher/dispatcher.py`:

```python
"""Public Dispatcher lifecycle and batch orchestration (spec §4.5)."""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Iterator, Sequence
from pathlib import Path

from .backends.base import ExecutionBackend
from .errors import BatchExistsError, BatchFailedError, DispatcherError, ProvisioningError
from .models import (
    Inventory,
    Job,
    JobHandle,
    JobResult,
    JobStatus,
    Project,
    ProvisioningReport,
    RetryPolicy,
)

_log = logging.getLogger(__name__)

_TERMINAL = frozenset({JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.TIMED_OUT})


class Dispatcher:
    """Public entry point: lifecycle + batch orchestration over an ExecutionBackend (§4.5)."""

    def __init__(
        self,
        inventory: Inventory,
        project: Project,
        *,
        results_dir: str = "./results",
        retry_policy: RetryPolicy = RetryPolicy(),  # noqa: B008
        raise_on_failure: bool = False,
        require_all_hosts: bool = True,
        backend: ExecutionBackend | None = None,
    ) -> None:
        self._inventory = inventory
        self._project = project
        self._results_dir = results_dir
        self._retry_policy = retry_policy
        self._raise_on_failure = raise_on_failure
        self._require_all_hosts = require_all_hosts
        if backend is None:
            from .backends.ssh_ray import SshRayBackend
            backend = SshRayBackend(
                retry_policy=retry_policy, results_dir=results_dir
            )
        self._backend = backend
        self._setup_done = False
        self._setup_report: ProvisioningReport | None = None
        self._outstanding: list[JobHandle] = []

    def setup(self, *, force: bool = False) -> ProvisioningReport:
        if self._setup_done:
            if force:
                raise DispatcherError(
                    "setup(force=True) is rejected after Ray has started; "
                    "teardown and create a new Dispatcher to re-provision (§4.5)"
                )
            return self._setup_report  # type: ignore[return-value]
        report = self._backend.setup(self._inventory, self._project)
        if self._require_all_hosts:
            failed = [h for h in report.hosts if not h.succeeded]
            if failed:
                raise ProvisioningError(report, f"{len(failed)} host(s) failed provisioning")
        self._setup_done = True
        self._setup_report = report
        return report

    def submit(self, jobs: Sequence[Job], *, batch_id: str | None = None) -> list[JobHandle]:
        if not self._setup_done:
            self.setup()
        if batch_id is None:
            batch_id = uuid.uuid4().hex
        batch_path = Path(self._results_dir) / batch_id
        if batch_path.exists():
            raise BatchExistsError(f"batch directory already exists: {batch_path}")
        handles = [self._backend.submit(batch_id, job) for job in jobs]
        self._outstanding.extend(handles)
        return handles

    def status(self, handle: JobHandle) -> JobStatus:
        return self._backend.status(handle)

    def cancel(self, handle: JobHandle) -> None:
        self._backend.cancel(handle)

    def as_completed(self, handles: Sequence[JobHandle]) -> Iterator[JobResult]:
        remaining = list(handles)
        while remaining:
            next_remaining: list[JobHandle] = []
            for h in remaining:
                if self._backend.status(h) in _TERMINAL:
                    yield self._backend.resolve(h)
                else:
                    next_remaining.append(h)
            remaining = next_remaining
            if remaining:
                time.sleep(0.1)  # ponytail: poll; push-based notification is §9.2 progress extra

    def run(self, jobs: Sequence[Job], *, batch_id: str | None = None) -> list[JobResult]:
        handles = self.submit(jobs, batch_id=batch_id)
        token_to_idx = {h.token: i for i, h in enumerate(handles)}
        ordered: list[JobResult | None] = [None] * len(handles)
        for result in self.as_completed(handles):
            # Match result back to its handle position via job_id.
            # Job IDs are assumed unique within a batch (spec §4.5).
            for h in handles:
                if h.job_id == result.id and ordered[token_to_idx[h.token]] is None:
                    ordered[token_to_idx[h.token]] = result
                    break
        results: list[JobResult] = ordered  # type: ignore[assignment]
        if self._raise_on_failure and any(r.status != JobStatus.SUCCEEDED for r in results):
            raise BatchFailedError(results)
        return results

    def teardown(self, *, purge: bool = False) -> None:
        if purge and self._outstanding:
            active = [
                h for h in self._outstanding
                if self._backend.status(h) not in _TERMINAL
            ]
            if active:
                raise DispatcherError(
                    f"teardown(purge=True) rejected: {len(active)} job(s) still active"
                )
        for h in self._outstanding:
            try:
                self._backend.cancel(h)
            except Exception:  # noqa: BLE001
                pass
        self._outstanding.clear()
        self._backend.teardown(purge=purge)
        self._setup_done = False
        self._setup_report = None

    def __enter__(self) -> "Dispatcher":
        return self

    def __exit__(self, *exc: object) -> None:
        try:
            self.teardown()
        except Exception as cleanup_exc:  # noqa: BLE001
            if exc[1] is not None:
                # Exception already in flight — log cleanup error, don't replace it.
                _log.exception("teardown error during context exit: %s", cleanup_exc)
            else:
                raise
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_dispatcher.py -v`
Expected: all tests pass.

If `test_run_returns_results_in_input_order` or `test_run_drains_all_before_raising` fail due to the job-id-to-handle matching logic, debug the `run()` method. The issue is that `as_completed` yields `JobResult` and we need to map it back to the right slot. With unique job IDs and the first-unused-slot match, it should work for the test's setup. If not, simplify by building `{job_id: result}` dict and then `[results_by_id[job.id] for job in jobs]` — this is cleaner and works as long as job IDs are unique.

Simplified `run()` (fallback if matching logic is fragile):
```python
def run(self, jobs: Sequence[Job], *, batch_id: str | None = None) -> list[JobResult]:
    handles = self.submit(jobs, batch_id=batch_id)
    results_by_id: dict[str, JobResult] = {}
    for result in self.as_completed(handles):
        results_by_id[result.id] = result
    ordered = [results_by_id[job.id] for job in jobs]
    if self._raise_on_failure and any(r.status != JobStatus.SUCCEEDED for r in ordered):
        raise BatchFailedError(ordered)
    return ordered
```

- [ ] **Step 5: Run gate checks**

Run: `uv run ruff check --fix . && uv run ruff check . && uv run mypy`
Expected: all clean.

- [ ] **Step 6: Commit**

```bash
git add src/ray_dispatcher/dispatcher.py tests/unit/test_dispatcher.py
git commit -m "feat: Dispatcher lifecycle + batch orchestration (§4.5)"
```

---

### Task 2: Integration test — Dispatcher end-to-end with monkeypatched backend

**Files:**
- Test: `tests/integration/test_dispatcher_e2e.py`

**Interfaces:**
- Consumes: `Dispatcher` (Task 1); `SshRayBackend` via monkeypatched `provision`; `FakeTransport`; `ProvisioningOutcome` from `provisioning`.
- Produces: integration test that runs a full batch through `Dispatcher.run()` using a live Ray runtime and FakeTransport.

This test is the first end-to-end smoke test of the full stack: `Dispatcher` → `SshRayBackend` → `_attempt_task` → `run_job` → `execute_attempt` → `FakeTransport`.

- [ ] **Step 1: Write the test**

Create `tests/integration/test_dispatcher_e2e.py`:

```python
import pytest

from ray_dispatcher.backends import ssh_ray
from ray_dispatcher.dispatcher import Dispatcher
from ray_dispatcher.errors import BatchExistsError
from ray_dispatcher.models import (
    HostProvisioningResult,
    Inventory,
    Job,
    JobStatus,
    Project,
    ProvisioningReport,
    RemoteHost,
)
from ray_dispatcher.provisioning import ProvisioningOutcome
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _project():
    return Project(path="/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25")


def _canned_outcome(*host_names: str) -> ProvisioningOutcome:
    report = ProvisioningReport(tuple(
        HostProvisioningResult(h, True, "src123", "env123") for h in host_names
    ))
    return ProvisioningOutcome(report, sessions={})


def _ok_transport(host: RemoteHost) -> FakeTransport:
    def results(argv: list[str]) -> CommandResult:
        cmd = " ".join(argv)
        if 'printf %s "$HOME"' in cmd:
            return CommandResult(0, "/home/ubuntu", "", 0.0)
        if argv[0] == "cat":
            return CommandResult(0, '{"returncode": 0, "duration_s": 0.05}', "", 0.0)
        return CommandResult(0, "", "", 0.0)
    return FakeTransport(run_results=results)


def test_dispatcher_run_succeeds_end_to_end(tmp_path, monkeypatch):
    """Full stack: Dispatcher.run → SshRayBackend → _attempt_task → FakeTransport."""
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=2),))
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: _canned_outcome("10.0.0.1"))

    from ray_dispatcher.backends.ssh_ray import SshRayBackend
    backend = SshRayBackend(transport_factory=_ok_transport, results_dir=str(tmp_path))

    jobs = [Job(id=f"job-{i}", command=("echo", str(i))) for i in range(3)]
    with Dispatcher(inv, _project(), backend=backend, results_dir=str(tmp_path)) as d:
        d.setup()
        results = d.run(jobs)

    assert len(results) == 3
    assert [r.id for r in results] == [j.id for j in jobs]  # input order
    for r in results:
        assert r.status == JobStatus.SUCCEEDED


def test_dispatcher_run_raises_batch_failed_when_raise_on_failure(tmp_path, monkeypatch):
    """raise_on_failure=True raises BatchFailedError after all jobs complete."""
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=1),))
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: _canned_outcome("10.0.0.1"))

    from ray_dispatcher.backends.ssh_ray import SshRayBackend
    from ray_dispatcher.errors import BatchFailedError

    def _fail_transport(host: RemoteHost) -> FakeTransport:
        def results(argv: list[str]) -> CommandResult:
            if 'printf %s "$HOME"' in " ".join(argv):
                return CommandResult(0, "/home/ubuntu", "", 0.0)
            if argv[0] == "cat":
                return CommandResult(0, '{"returncode": 1, "duration_s": 0.05}', "", 0.0)
            return CommandResult(0, "", "", 0.0)
        return FakeTransport(run_results=results)

    backend = SshRayBackend(transport_factory=_fail_transport, results_dir=str(tmp_path))
    with pytest.raises(BatchFailedError) as exc_info:
        with Dispatcher(inv, _project(), backend=backend, raise_on_failure=True,
                        results_dir=str(tmp_path)) as d:
            d.setup()
            d.run([Job(id="j1", command=("fail",))])
    assert len(exc_info.value.results) == 1


def test_dispatcher_submit_raises_batch_exists_error(tmp_path, monkeypatch):
    """BatchExistsError raised if batch dir already exists (§4.5)."""
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=1),))
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: _canned_outcome("10.0.0.1"))

    from ray_dispatcher.backends.ssh_ray import SshRayBackend
    backend = SshRayBackend(transport_factory=_ok_transport, results_dir=str(tmp_path))

    batch_id = "existing-batch"
    (tmp_path / batch_id).mkdir()
    with Dispatcher(inv, _project(), backend=backend, results_dir=str(tmp_path)) as d:
        d.setup()
        with pytest.raises(BatchExistsError):
            d.submit([Job(id="j1", command=("echo",))], batch_id=batch_id)
```

- [ ] **Step 2: Run to verify tests pass**

Run: `uv run pytest tests/integration/test_dispatcher_e2e.py -v`
Expected: all three tests pass.

- [ ] **Step 3: Run ALL tests**

Run: `uv run pytest -q`
Expected: all tests pass (249 + new = 252+).

- [ ] **Step 4: Run gate checks**

Run: `uv run ruff check --fix . && uv run ruff check . && uv run mypy`
Expected: all clean.

- [ ] **Step 5: Commit**

```bash
git add tests/integration/test_dispatcher_e2e.py
git commit -m "test: Dispatcher e2e integration tests (§4.5)"
```

---

### Task 3: `__init__.py` exports — add Dispatcher and SshRayBackend

**Files:**
- Modify: `src/ray_dispatcher/__init__.py`

**Interfaces:**
- Consumes: `Dispatcher` from `dispatcher.py` (Task 1); `SshRayBackend` from `backends/ssh_ray.py`.
- Produces: `from ray_dispatcher import Dispatcher, SshRayBackend` works.

- [ ] **Step 1: Write the failing test**

This is a one-liner change, so verify by grep before and after rather than a new test file. Instead, verify the import works at the module level:

```bash
uv run python -c "from ray_dispatcher import Dispatcher, SshRayBackend; print('ok')"
```
Expected before edit: `ImportError: cannot import name 'Dispatcher'`.

- [ ] **Step 2: Write the implementation**

Edit `src/ray_dispatcher/__init__.py`:

Add to imports (after the `from .models import ...` block):
```python
from .backends.ssh_ray import SshRayBackend
from .dispatcher import Dispatcher
```

Add to `__all__` list (after `"BatchFailedError"` entry):
```python
    # high-level API
    "Dispatcher",
    "SshRayBackend",
```

- [ ] **Step 3: Verify import works**

Run: `uv run python -c "from ray_dispatcher import Dispatcher, SshRayBackend; print('ok')"`
Expected: `ok`

- [ ] **Step 4: Run gate checks**

Run: `uv run ruff check --fix . && uv run ruff check . && uv run mypy`
Expected: all clean. Note: importing `SshRayBackend` in `__init__.py` triggers `import ray` at import time (since `ssh_ray.py` imports ray at module level). This is acceptable — `ray` is a required dependency.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/__init__.py
git commit -m "feat: export Dispatcher and SshRayBackend from public __init__.py"
```

---

### Task 4: Phase 6d gate

**Files:** none new — full toolchain verification only.

- [ ] **Step 1: Run the full suite**

Run: `uv run pytest -q`
Expected: all tests pass (252+). The Ray FutureWarning is benign.

- [ ] **Step 2: Lint + type check**

Run: `uv run ruff check --fix . && uv run ruff check . && uv run mypy`
Expected: ruff `All checks passed!`; mypy `Success: no issues found`.

- [ ] **Step 3: Verify public API is complete**

Run:
```bash
uv run python -c "
from ray_dispatcher import (
    Dispatcher, SshRayBackend,
    Inventory, RemoteHost, Project, Job, JobStatus,
    DispatcherError, BatchExistsError, BatchFailedError,
)
print('all imports ok')
"
```
Expected: `all imports ok`

- [ ] **Step 4: Commit (only if Step 2 made ruff fixes)**

```bash
git add -A
git commit -m "chore: phase 6d gate green (ruff + mypy)"
```

---

## Phase 6d self-review

Run before declaring Phase 6d done:

- [ ] `Dispatcher.__init__` accepts `backend=None` and constructs `SshRayBackend` lazily; does NOT import `SshRayBackend` at module top-level (deferred import inside `__init__` avoids circular imports and delays the `import ray` side-effect).
- [ ] `setup()` is idempotent (second call returns cached report, no backend call); `setup(force=True)` after setup raises `DispatcherError`; `require_all_hosts=True` raises `ProvisioningError` on any failed host.
- [ ] `submit()` auto-calls `setup()` if not done; generates batch_id as UUID hex when `None`; raises `BatchExistsError` if `Path(results_dir)/batch_id` exists.
- [ ] `as_completed()` polls every 0.1 s, yields `JobResult` in completion order, terminates when all handles are resolved.
- [ ] `run()` returns results in **input order**; drains all jobs before raising `BatchFailedError` if `raise_on_failure=True`.
- [ ] `teardown()` cancels all outstanding handles (best-effort), delegates to `backend.teardown`; raises `DispatcherError` if `purge=True` and any handle still active.
- [ ] `__enter__` returns self with NO network call; `__exit__` calls `teardown()`, logs+swallows cleanup errors if exception already in flight.
- [ ] `dispatcher.py` does NOT import ray at module level.
- [ ] `__init__.py` exports `Dispatcher` and `SshRayBackend` in `__all__`.
- [ ] `uv run pytest -q`, `uv run ruff check .`, `uv run mypy` all green.

**Deliverable:** `from ray_dispatcher import Dispatcher` works; the usage sketch from §4.5 (`with Dispatcher(...) as d: results = d.run(jobs)`) runs end-to-end.

**Residuals carried to Phase 7:**
- Multipass e2e tests (§11): real VMs, real SSH.
- §9.2 Rich progress rendering (`progress` extra).
- §3.2.6 stale-lock reconciliation at provisioning takeover.
- `teardown(purge=True)` remote state deletion.
