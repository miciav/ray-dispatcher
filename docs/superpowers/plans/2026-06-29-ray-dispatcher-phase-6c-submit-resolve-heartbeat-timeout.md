# Phase 6c — submit/resolve/status/cancel + Ray attempt task + heartbeat + timeout

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `SshRayBackend` fully executable: `submit()` dispatches a `@ray.remote(num_cpus=0, resources={"vm_slot": 1}, max_retries=0)` task that wires `run_job` through a lease handle with live heartbeating; `resolve()`/`status()`/`cancel()` map `JobHandle` ↔ `ObjectRef`; timeout is enforced inside `execute_attempt` (§8.1); every job terminates as a `JobResult` (catch-all for the Phase 6a residual).

**Architecture:** `_attempt_task` is a module-level `@ray.remote` function in `ssh_ray.py`; it receives serializable params (HostRuntime dict, RemoteHost dict, actor handle, transport factory, RetryPolicy, JobLayout), creates `_ActorLeaseHandle` with a heartbeat thread, calls `run_job`, writes `result.json`, and wraps all exceptions in a `INTERNAL`/`CANCELLED` `JobResult`. `_ActorLeaseHandle` starts a daemon heartbeat thread on `acquire` and stops it before `release`. `SshRayBackend` stores a `dict[str, object]` of ObjectRefs keyed by handle token; `status()` uses `ray.wait(..., timeout=0)` to distinguish RUNNING from terminal. Timeout in `execute_attempt` runs the runner invocation in a daemon thread, reads the remote PID file on expiry, and calls `terminate_process_group`.

**Tech Stack:** Python 3.10+, `ray>=2.40,<3`, `threading` (stdlib), `uuid` (stdlib). Existing: `run_job`, `execute_attempt`, `LeaseService.heartbeat`, `write_result_json`, `JobLayout`, `terminate_process_group`, `RemoteHost`, `HostRuntime`.

## Global Constraints

- Python floor 3.10; `from __future__ import annotations` at the top of every modified module.
- mypy strict (`files=["src"]`) and ruff (`E/F/I/UP/B`, line-length 100) must pass with zero errors.
- **§3.2.3 (binding):** `_attempt_task` decorated with exactly `@ray.remote(num_cpus=0, resources={"vm_slot": 1}, max_retries=0)` — Ray must never auto-retry a failed attempt (§11).
- **§8.2 (binding):** `_ActorLeaseHandle` starts a daemon heartbeat thread when `acquire` returns; the thread calls `ray.get(actor.heartbeat.remote(token))` every `heartbeat_interval_s` seconds and stops when `release` sets the stop event. `heartbeat_interval_s` defaults to 5.0 (< lease_ttl_s/2 = 30.0).
- **§8.1 (binding):** `execute_attempt` enforces `job.timeout_s` by running the SSH runner invocation in a daemon thread; on expiry it reads the remote PID file (`run.pid`), calls `terminate_process_group`, and returns a `TIMEOUT` AttemptResult.
- **Catch-all (§6a residual):** every job submitted via `_attempt_task` terminates as a `JobResult`, never as a raw exception. `ray.exceptions.TaskCancelledError` produces `CANCELLED`; any other `Exception` produces `FAILED` with `error="INTERNAL: ..."`.
- Only `ssh_ray.py` imports `ray`. `scheduling.py` remains Ray-free (it only gains `import threading`).
- Data types already defined (do NOT redefine): `Job`, `JobHandle`, `JobResult`, `JobStatus`, `RemoteHost`, `HostRuntime`, `RetryPolicy`, `ProvisioningReport` (`models.py`); `run_job`, `execute_attempt`, `LeaseService` (`scheduling.py`); `JobLayout`, `write_result_json` (`results.py`); `terminate_process_group` (`ssh.py`).

**Deferred to Phase 6c/6d (do NOT build here):** full cancel/reconcile of outstanding attempts on `teardown`; purge; `Dispatcher` lifecycle; status registry as a separate Ray actor; `BatchExistsError`; `__init__.py` exports.

---

### Task 1: Heartbeat in `_ActorLeaseHandle` (§8.2)

**Files:**
- Modify: `src/ray_dispatcher/backends/ssh_ray.py` (`_ActorLeaseHandle` only)
- Modify: `tests/integration/test_host_lease_actor.py` (add heartbeat test)

**Interfaces:**
- Consumes: `LeaseService.heartbeat(token: str) -> bool` (already exists — async actor method); `ray.get(actor.heartbeat.remote(token)) -> bool`; `threading.Thread`, `threading.Event`.
- Produces: `_ActorLeaseHandle(actor, *, heartbeat_interval_s: float = 5.0)` — same `acquire`/`release` signatures as before, but `acquire` now starts a daemon thread that heartbeats every `heartbeat_interval_s` seconds, and `release` stops the thread before calling the actor.

- [ ] **Step 1: Write the failing test**

Add to `tests/integration/test_host_lease_actor.py`:

```python
import time

def test_actor_lease_handle_heartbeats_while_held():
    ray.init(address="local", namespace="test-heartbeat", num_cpus=2)
    try:
        # Short TTL so an un-heartbeated lease would expire quickly.
        actor = HostLease.remote({"a": 1}, lease_ttl_s=1.0)
        handle = _ActorLeaseHandle(actor, heartbeat_interval_s=0.1)
        lease = handle.acquire("job/hb-test")
        time.sleep(0.5)  # let several heartbeats fire (5 × 0.1s)
        # If heartbeat fired, the lease is still alive in the actor's pool.
        alive = ray.get(actor.heartbeat.remote(lease.token))
        assert alive, "heartbeat must keep the lease alive"
        handle.release(lease.token)
        # After release, the token is gone.
        gone = not ray.get(actor.heartbeat.remote(lease.token))
        assert gone, "token must not exist after release"
    finally:
        ray.shutdown()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_host_lease_actor.py::test_actor_lease_handle_heartbeats_while_held -v`
Expected: FAIL — `_ActorLeaseHandle.__init__` does not yet accept `heartbeat_interval_s`.

- [ ] **Step 3: Write the implementation**

Replace the entire `_ActorLeaseHandle` class in `ssh_ray.py` (keep everything else). Add `import threading` to the stdlib import group (before `import ray` — ruff orders plain `import` stdlib before third-party).

```python
class _ActorLeaseHandle:
    """Synchronous LeaseHandle over the async HostLease actor; heartbeats while the lease is held (§8.2)."""

    def __init__(
        self,
        actor: ray.actor.ActorHandle[LeaseService],  # type: ignore[type-arg]
        *,
        heartbeat_interval_s: float = 5.0,
    ) -> None:
        self._actor = actor
        self._heartbeat_interval_s = heartbeat_interval_s
        self._stop = threading.Event()
        self._hb_thread: threading.Thread | None = None

    def acquire(self, attempt_id: str, *, exclude: Iterable[str] = ()) -> Lease:
        lease = ray.get(self._actor.acquire.remote(attempt_id, exclude=tuple(exclude)))  # type: ignore[no-any-return]
        self._stop.clear()
        self._hb_thread = threading.Thread(
            target=self._heartbeat, args=(lease.token,), daemon=True, name="lease-heartbeat"
        )
        self._hb_thread.start()
        return lease

    def release(self, token: str) -> None:
        self._stop.set()
        if self._hb_thread is not None:
            self._hb_thread.join(timeout=self._heartbeat_interval_s + 1.0)
            self._hb_thread = None
        ray.get(self._actor.release.remote(token))

    def _heartbeat(self, token: str) -> None:
        while not self._stop.wait(self._heartbeat_interval_s):
            try:
                alive: bool = ray.get(self._actor.heartbeat.remote(token))  # type: ignore[assignment]
            except Exception:
                break
            if not alive:
                break
```

Note: the existing `test_host_lease_actor.py` test creates `_ActorLeaseHandle(actor)` without the new keyword arg — the default `heartbeat_interval_s=5.0` keeps it compatible. The existing test must still pass.

- [ ] **Step 4: Run both tests to verify they pass**

Run: `uv run pytest tests/integration/test_host_lease_actor.py -v`
Expected: both tests pass (the original roundtrip and the new heartbeat test).

- [ ] **Step 5: Run gate checks**

Run: `uv run ruff check --fix . && uv run ruff check . && uv run mypy`
Expected: all clean.

- [ ] **Step 6: Commit**

```bash
git add src/ray_dispatcher/backends/ssh_ray.py tests/integration/test_host_lease_actor.py
git commit -m "feat: _ActorLeaseHandle heartbeats HostLease actor while lease is held (§8.2)"
```

---

### Task 2: `_attempt_task` module-level Ray remote function + catch-all + write result.json

**Files:**
- Modify: `src/ray_dispatcher/backends/ssh_ray.py` (add `_attempt_task` function and new imports)
- Test: `tests/unit/test_attempt_task.py`

**Interfaces:**
- Consumes: `_ActorLeaseHandle(actor, *, heartbeat_interval_s)` (Task 1); `run_job(job, *, batch_id, lease, runtime_for, transport_for, local, policy) -> JobResult` (`scheduling.py`); `write_result_json(path, result)` (`results.py`); `JobLayout(results_dir, batch_id, job_id)` (`results.py`); `ray.exceptions.TaskCancelledError`.
- Produces: module-level `_attempt_task` decorated with `@ray.remote(num_cpus=0, resources={"vm_slot": 1}, max_retries=0)`. Its `.remote(...)` call is used by `SshRayBackend.submit` (Task 3).

**Signature:**
```python
@ray.remote(num_cpus=0, resources={"vm_slot": 1}, max_retries=0)
def _attempt_task(
    job: Job,
    batch_id: str,
    local: JobLayout,
    runtimes: dict[str, HostRuntime],
    inv_hosts: dict[str, RemoteHost],
    actor: ray.actor.ActorHandle,  # type: ignore[type-arg]
    transport_factory: Callable[[RemoteHost], Transport],
    policy: RetryPolicy,
    heartbeat_interval_s: float = 5.0,
) -> JobResult:
```

- [ ] **Step 1: Write the failing test**

`tests/unit/test_attempt_task.py`:

This test exercises `_attempt_task` directly (not via Ray) by calling its underlying function. Since `@ray.remote` wraps the function, we can access the original via `_attempt_task._function` (Ray's `RemoteFunction` stores it there) or we can verify the Ray decoration attributes. However, the catch-all behavior is simplest to test by calling `_attempt_task.remote(...)` in a live local Ray runtime.

```python
import pytest
import ray

from ray_dispatcher.backends.ssh_ray import HostLease, _attempt_task
from ray_dispatcher.models import (
    HostProvisioningResult,
    Inventory,
    Job,
    Project,
    ProvisioningReport,
    RemoteHost,
    RetryPolicy,
)
from ray_dispatcher.provisioning import RemoteLayout
from ray_dispatcher.results import JobLayout
from ray_dispatcher.scheduling import HostRuntime
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _project():
    return Project(path="/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25")


def _ok_transport(host: RemoteHost) -> FakeTransport:
    """FakeTransport that answers all execute_attempt commands with success."""
    def results(argv: list[str]) -> CommandResult:
        if argv[0] == "cat":  # read result.json
            return CommandResult(0, '{"returncode": 0, "duration_s": 0.1}', "", 0.0)
        return CommandResult(0, "", "", 0.0)
    return FakeTransport(run_results=results)


def _canned_runtime(host_name: str) -> HostRuntime:
    layout = RemoteLayout("/home/ubuntu", "dfaas")
    return HostRuntime(
        host=host_name,
        layout=layout,
        environment_digest="env123",
        runner_digest="run123",
        project_path="/proj",
        secret_env={},
    )


def test_attempt_task_has_correct_ray_decoration():
    """Verify the task declares the exact Ray options from §3.2.3 and §11."""
    opts = _attempt_task._remote_kwargs  # type: ignore[attr-defined]
    assert opts.get("num_cpus") == 0
    assert opts.get("resources") == {"vm_slot": 1}
    assert opts.get("max_retries") == 0


def test_attempt_task_succeeds_with_fake_transport(tmp_path):
    ray.init(address="local", namespace="test-task-ok", resources={"vm_slot": 2.0})
    try:
        host = RemoteHost("10.0.0.1", user="ubuntu", slots=1)
        actor = HostLease.remote({"10.0.0.1": 1})
        runtimes = {"10.0.0.1": _canned_runtime("10.0.0.1")}
        inv_hosts = {"10.0.0.1": host}
        local = JobLayout(str(tmp_path), "batch1", "job1")
        job = Job(id="job1", command=("echo", "hi"))

        ref = _attempt_task.remote(  # type: ignore[attr-defined]
            job, "batch1", local, runtimes, inv_hosts, actor, _ok_transport, RetryPolicy(),
            heartbeat_interval_s=1.0,
        )
        result = ray.get(ref)
        from ray_dispatcher.models import JobStatus
        assert result.status == JobStatus.SUCCEEDED
        assert result.id == "job1"
        # result.json written to disk
        assert local.result_json.exists()
    finally:
        ray.shutdown()


def test_attempt_task_catch_all_returns_internal_on_unexpected_exception(tmp_path):
    """An exception escaping run_job (e.g. malformed result.json) becomes INTERNAL, not a crash."""
    ray.init(address="local", namespace="test-task-internal", resources={"vm_slot": 2.0})
    try:
        host = RemoteHost("10.0.0.1", user="ubuntu", slots=1)
        actor = HostLease.remote({"10.0.0.1": 1})
        runtimes = {"10.0.0.1": _canned_runtime("10.0.0.1")}
        inv_hosts = {"10.0.0.1": host}
        local = JobLayout(str(tmp_path), "batch1", "job2")
        job = Job(id="job2", command=("echo", "hi"))

        def _bad_transport(h: RemoteHost) -> FakeTransport:
            def results(argv: list[str]) -> CommandResult:
                if argv[0] == "cat":
                    return CommandResult(0, "not-json!", "", 0.0)  # malformed result.json
                return CommandResult(0, "", "", 0.0)
            return FakeTransport(run_results=results)

        ref = _attempt_task.remote(  # type: ignore[attr-defined]
            job, "batch1", local, runtimes, inv_hosts, actor, _bad_transport, RetryPolicy(),
        )
        result = ray.get(ref)
        from ray_dispatcher.models import JobStatus
        assert result.status == JobStatus.FAILED
        assert result.error is not None and "INTERNAL" in result.error
    finally:
        ray.shutdown()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_attempt_task.py -v`
Expected: FAIL — `cannot import name '_attempt_task'`.

- [ ] **Step 3: Write the implementation**

Add the following imports to the import block in `ssh_ray.py`:
- stdlib: `import uuid` (add to existing stdlib group; ruff sorts alphabetically)
- `from ..results import JobLayout, write_result_json`
- add `run_job` to the scheduling import line: `from ..scheduling import HostRuntime, Lease, LeaseService, run_job, secret_env_map`

Add `_attempt_task` BEFORE the `SshRayBackend` class (after `_ActorLeaseHandle`):

```python
@ray.remote(num_cpus=0, resources={"vm_slot": 1}, max_retries=0)
def _attempt_task(
    job: Job,
    batch_id: str,
    local: JobLayout,
    runtimes: dict[str, HostRuntime],
    inv_hosts: dict[str, RemoteHost],
    actor: ray.actor.ActorHandle,  # type: ignore[type-arg]
    transport_factory: Callable[[RemoteHost], Transport],
    policy: RetryPolicy,
    heartbeat_interval_s: float = 5.0,
) -> JobResult:
    """Ray task: run one logical job via run_job; every execution terminates as a JobResult (§3.2.3, §6a residual)."""
    lease_handle = _ActorLeaseHandle(actor, heartbeat_interval_s=heartbeat_interval_s)
    try:
        result = run_job(
            job,
            batch_id=batch_id,
            lease=lease_handle,
            runtime_for=runtimes.__getitem__,
            transport_for=lambda host: transport_factory(inv_hosts[host]),
            local=local,
            policy=policy,
        )
        write_result_json(local.result_json, result)
        return result
    except ray.exceptions.TaskCancelledError:
        cancelled = JobResult(
            id=job.id, batch_id=batch_id, status=JobStatus.CANCELLED,
            returncode=None, duration_s=0.0, host=None, output_dir=None,
            attempts=(), error="cancelled",
        )
        write_result_json(local.result_json, cancelled)
        return cancelled
    except Exception as exc:
        internal = JobResult(
            id=job.id, batch_id=batch_id, status=JobStatus.FAILED,
            returncode=None, duration_s=0.0, host=None, output_dir=None,
            attempts=(), error=f"INTERNAL: {type(exc).__name__}: {exc}",
        )
        write_result_json(local.result_json, internal)
        return internal
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_attempt_task.py -v`
Expected: all three tests pass.

- [ ] **Step 5: Run gate checks**

Run: `uv run ruff check --fix . && uv run ruff check . && uv run mypy`
Expected: all clean. If mypy flags `_attempt_task._remote_kwargs` in the test file, that is a test file — not under `files=["src"]` — so mypy ignores it.

- [ ] **Step 6: Commit**

```bash
git add src/ray_dispatcher/backends/ssh_ray.py tests/unit/test_attempt_task.py
git commit -m "feat: add _attempt_task Ray remote function with catch-all and result.json write (§3.2.3, §6a residual)"
```

---

### Task 3: `results_dir`, `_inv_hosts`, `_refs` in `SshRayBackend` + `submit`/`status`/`cancel`/`resolve`

**Files:**
- Modify: `src/ray_dispatcher/backends/ssh_ray.py` (`SshRayBackend.__init__`, `setup`, and the four method bodies)
- Test: `tests/integration/test_backend_submit.py`

**Interfaces:**
- Consumes: `_attempt_task.remote(...)` (Task 2); `JobLayout(results_dir, batch_id, job_id)`; `ray.wait([ref], timeout=0)`, `ray.get(ref)`, `ray.cancel(ref)`, `ray.exceptions.TaskCancelledError`; `uuid.uuid4().hex`.
- Produces:
  - `SshRayBackend(*, results_dir="./results", ...)` — new `results_dir` kwarg.
  - `SshRayBackend.setup(inventory, project)` additionally stores `self._inv_hosts: dict[str, RemoteHost]` and initializes `self._refs: dict[str, object] = {}`.
  - `submit(batch_id, job) -> JobHandle` — creates JobLayout, calls `_attempt_task.remote`, stores ref.
  - `status(handle) -> JobStatus` — RUNNING if ref not ready, terminal status from result if ready.
  - `cancel(handle) -> None` — calls `ray.cancel(ref)`.
  - `resolve(handle) -> JobResult` — `ray.get(ref)`, catches `TaskCancelledError` → CANCELLED result.

- [ ] **Step 1: Write the failing test**

`tests/integration/test_backend_submit.py`:

```python
import threading
import time

import pytest
import ray

from ray_dispatcher.backends import ssh_ray
from ray_dispatcher.backends.ssh_ray import SshRayBackend
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
        if 'printf %s "$HOME"' in " ".join(argv):
            return CommandResult(0, "/home/ubuntu", "", 0.0)
        if argv[0] == "cat":
            return CommandResult(0, '{"returncode": 0, "duration_s": 0.05}', "", 0.0)
        return CommandResult(0, "", "", 0.0)
    return FakeTransport(run_results=results)


def _fail_transport(host: RemoteHost) -> FakeTransport:
    """FakeTransport that reports returncode=1 (COMMAND failure)."""
    def results(argv: list[str]) -> CommandResult:
        if 'printf %s "$HOME"' in " ".join(argv):
            return CommandResult(0, "/home/ubuntu", "", 0.0)
        if argv[0] == "cat":
            return CommandResult(0, '{"returncode": 1, "duration_s": 0.05}', "", 0.0)
        return CommandResult(0, "", "", 0.0)
    return FakeTransport(run_results=results)


def test_submit_and_resolve_succeeds(tmp_path, monkeypatch):
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=2),))
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: _canned_outcome("10.0.0.1"))
    backend = SshRayBackend(transport_factory=_ok_transport, results_dir=str(tmp_path))
    try:
        backend.setup(inv, _project())
        job = Job(id="j1", command=("echo", "hi"))
        handle = backend.submit("batch1", job)
        result = backend.resolve(handle)
        assert result.status == JobStatus.SUCCEEDED
        assert result.id == "j1"
    finally:
        backend.teardown()


def test_submit_failed_command_returns_failed_not_ray_retry(tmp_path, monkeypatch):
    """COMMAND failure: result has 1 attempt (max_retries=0 — Ray never auto-retries) (§11)."""
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=1),))
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: _canned_outcome("10.0.0.1"))
    backend = SshRayBackend(transport_factory=_fail_transport, results_dir=str(tmp_path))
    try:
        backend.setup(inv, _project())
        job = Job(id="j2", command=("failing",))
        handle = backend.submit("batch1", job)
        result = backend.resolve(handle)
        assert result.status == JobStatus.FAILED
        assert len(result.attempts) == 1  # no retry for COMMAND failure
    finally:
        backend.teardown()


def test_status_returns_running_then_succeeded(tmp_path, monkeypatch):
    """status() returns RUNNING while the task blocks, then SUCCEEDED after resolve."""
    block = threading.Event()

    def _blocking_transport(host: RemoteHost) -> FakeTransport:
        def results(argv: list[str]) -> CommandResult:
            if 'printf %s "$HOME"' in " ".join(argv):
                return CommandResult(0, "/home/ubuntu", "", 0.0)
            if "python3" in " ".join(argv):  # runner invocation
                block.wait(timeout=10.0)
                return CommandResult(0, "", "", 0.0)
            if argv[0] == "cat":
                return CommandResult(0, '{"returncode": 0, "duration_s": 0.1}', "", 0.0)
            return CommandResult(0, "", "", 0.0)
        return FakeTransport(run_results=results)

    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=1),))
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: _canned_outcome("10.0.0.1"))
    backend = SshRayBackend(transport_factory=_blocking_transport, results_dir=str(tmp_path))
    try:
        backend.setup(inv, _project())
        job = Job(id="j3", command=("sleep",))
        handle = backend.submit("batch1", job)
        # Poll until RUNNING (Ray task takes a moment to start).
        for _ in range(40):
            if backend.status(handle) == JobStatus.RUNNING:
                break
            time.sleep(0.05)
        assert backend.status(handle) == JobStatus.RUNNING
        block.set()  # unblock the transport
        result = backend.resolve(handle)
        assert result.status == JobStatus.SUCCEEDED
    finally:
        backend.teardown()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_backend_submit.py -v`
Expected: FAIL — `submit`, `status`, `resolve` raise `NotImplementedError`.

- [ ] **Step 3: Write the implementation**

**3a. Update `SshRayBackend.__init__`:** add `results_dir: str = "./results"` kwarg and two new instance attrs.

```python
def __init__(
    self,
    *,
    runner_path: str | None = None,
    transport_factory: Callable[..., Transport] | None = None,
    retry_policy: RetryPolicy = RetryPolicy(),  # noqa: B008
    min_disk_mb: int = 500,
    results_dir: str = "./results",
) -> None:
    from .. import remote_runner

    self._runner_path = runner_path or remote_runner.__file__
    self._transport_factory = transport_factory or _default_transport
    self._retry_policy = retry_policy
    self._min_disk_mb = min_disk_mb
    self._results_dir = results_dir
    self._owns_runtime = False
    self._actor: ray.actor.ActorHandle | None = None  # type: ignore[type-arg]
    self._outcome: ProvisioningOutcome | None = None
    self._runtimes: dict[str, HostRuntime] = {}
    self._inv_hosts: dict[str, RemoteHost] = {}
    self._refs: dict[str, object] = {}
```

**3b. Update `setup()`:** after building `inv_hosts`, store it:

```python
        inv_hosts = {h.host: h for h in inventory.hosts}
        inv_slots = {name: inv_hosts[name].slots for name in healthy}
        slots = inv_slots
        self._inv_hosts = inv_hosts          # ← add this line
        self._runtimes = { ... }             # unchanged
```

**3c. Replace the four method stubs** with real implementations:

```python
    def submit(self, batch_id: str, job: Job) -> JobHandle:
        token = uuid.uuid4().hex
        local = JobLayout(self._results_dir, batch_id, job.id)
        ref = _attempt_task.remote(  # type: ignore[attr-defined]
            job, batch_id, local, self._runtimes, self._inv_hosts,
            self._actor, self._transport_factory, self._retry_policy,
        )
        self._refs[token] = ref
        return JobHandle(batch_id=batch_id, job_id=job.id, token=token)

    def status(self, handle: JobHandle) -> JobStatus:
        ref = self._refs.get(handle.token)
        if ref is None:
            return JobStatus.PENDING
        ready, _ = ray.wait([ref], timeout=0)  # type: ignore[arg-type]
        if ready:
            result: JobResult = ray.get(ref)  # type: ignore[assignment]
            return result.status
        return JobStatus.RUNNING

    def cancel(self, handle: JobHandle) -> None:
        ref = self._refs.get(handle.token)
        if ref is not None:
            ray.cancel(ref)  # type: ignore[arg-type]

    def resolve(self, handle: JobHandle) -> JobResult:
        ref = self._refs[handle.token]
        try:
            return ray.get(ref)  # type: ignore[no-any-return]
        except ray.exceptions.TaskCancelledError:
            return JobResult(
                id=handle.job_id, batch_id=handle.batch_id, status=JobStatus.CANCELLED,
                returncode=None, duration_s=0.0, host=None, output_dir=None,
                attempts=(), error="cancelled",
            )
```

Also ensure `import uuid` is at the top of `ssh_ray.py` in the stdlib group.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_backend_submit.py -v`
Expected: all three tests pass.

- [ ] **Step 5: Run gate checks**

Run: `uv run ruff check --fix . && uv run ruff check . && uv run mypy`
Expected: all clean. If mypy flags `ray.wait`, `ray.get`, `ray.cancel` argument types, add minimal `# type: ignore[arg-type]` / `# type: ignore[no-any-return]` / `# type: ignore[assignment]` with the exact error code.

- [ ] **Step 6: Commit**

```bash
git add src/ray_dispatcher/backends/ssh_ray.py tests/integration/test_backend_submit.py
git commit -m "feat: SshRayBackend submit/status/cancel/resolve + results_dir (§3.3, §9.2)"
```

---

### Task 4: Timeout enforcement in `execute_attempt` (§8.1)

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (add `import threading`; update the runner invocation block)
- Test: `tests/unit/test_execute_attempt_timeout.py`

**Interfaces:**
- Consumes: `threading.Thread`, `threading.Event`; `job.timeout_s: float | None`; `run.pid` (the remote PID file path, written by remote_runner as `{"pid": N, "pgid": N}`); `terminate_process_group(transport, pgid, grace_s=10.0)` (already in `ssh.py` and imported in `scheduling.py`).
- Produces: `execute_attempt` returns `AttemptResult(status=TIMED_OUT, failure_kind=TIMEOUT)` when the runner SSH call exceeds `job.timeout_s`. When `job.timeout_s is None`, behaviour is unchanged.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_execute_attempt_timeout.py`:

```python
import threading
import time

import pytest

from ray_dispatcher.models import FailureKind, Job, JobStatus, RemoteHost
from ray_dispatcher.provisioning import RemoteLayout
from ray_dispatcher.scheduling import HostRuntime, execute_attempt
from ray_dispatcher.results import JobLayout
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _runtime(host: str = "10.0.0.1") -> HostRuntime:
    layout = RemoteLayout("/home/ubuntu", "dfaas")
    return HostRuntime(
        host=host,
        layout=layout,
        environment_digest="env123",
        runner_digest="run123",
        project_path="/proj",
        secret_env={},
    )


def test_execute_attempt_returns_timeout_when_runner_exceeds_timeout_s(tmp_path):
    """Timeout path: runner blocks past timeout_s → TIMED_OUT AttemptResult (§8.1)."""
    runner_started = threading.Event()
    runner_done = threading.Event()

    def results(argv: list[str]) -> CommandResult:
        cmd = " ".join(argv)
        if "python3" in cmd:  # runner invocation — block until released
            runner_started.set()
            runner_done.wait(timeout=5.0)
            return CommandResult(0, "", "", 0.0)
        if argv[0] == "cat" and "pid" in cmd:  # PID file read
            return CommandResult(0, '{"pid": 111, "pgid": 222}', "", 0.0)
        if "kill" in cmd and "-0" in cmd:  # terminate_process_group probe
            runner_done.set()
            return CommandResult(1, "", "", 0.0)  # pgid already gone → terminates immediately
        return CommandResult(0, "", "", 0.0)

    transport = FakeTransport(run_results=results)
    layout = JobLayout(str(tmp_path), "b1", "j1")
    job = Job(id="j1", command=("sleep", "99"), timeout_s=0.05)  # very short timeout

    result_holder: list = []

    def run():
        result_holder.append(
            execute_attempt(transport, _runtime(), job, batch_id="b1", attempt=1, local=layout)
        )

    t = threading.Thread(target=run)
    t.start()
    runner_started.wait(timeout=5.0)  # wait for runner to start
    t.join(timeout=10.0)

    assert len(result_holder) == 1
    r = result_holder[0]
    assert r.status == JobStatus.TIMED_OUT
    assert r.failure_kind == FailureKind.TIMEOUT
    assert r.returncode is None


def test_execute_attempt_no_timeout_when_timeout_s_is_none(tmp_path):
    """When timeout_s is None, the runner is invoked without threading (§8.1 baseline)."""
    def results(argv: list[str]) -> CommandResult:
        if argv[0] == "cat":
            return CommandResult(0, '{"returncode": 0, "duration_s": 0.0}', "", 0.0)
        return CommandResult(0, "", "", 0.0)

    transport = FakeTransport(run_results=results)
    layout = JobLayout(str(tmp_path), "b1", "j2")
    job = Job(id="j2", command=("echo", "hi"))  # timeout_s=None by default

    result = execute_attempt(transport, _runtime(), job, batch_id="b1", attempt=1, local=layout)
    assert result.status == JobStatus.SUCCEEDED
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_execute_attempt_timeout.py -v`
Expected: `test_execute_attempt_no_timeout_when_timeout_s_is_none` passes (baseline unchanged); `test_execute_attempt_returns_timeout_when_runner_exceeds_timeout_s` hangs or fails (no timeout enforcement yet).

If the first test hangs (the runner blocks indefinitely without timeout enforcement), stop the run with Ctrl-C and confirm the second test passes. This confirms the baseline is intact and the timeout path is missing.

- [ ] **Step 3: Write the implementation**

Add `import threading` to `scheduling.py`'s stdlib import group (alphabetically between `import time` and any later stdlib import — ruff will fix order if needed).

Replace the runner invocation block in `execute_attempt` (the single `_run_checked(transport, ["python3", runner, run.manifest], "invoke runner")` line, which currently has the comment "# No timeout here: enforcement + termination are Phase 6 (§8.1).") with:

```python
    # §7.6 invoke the versioned runner. Timeout enforcement §8.1:
    # run in a daemon thread so we can terminate the remote process if timeout_s elapses.
    if job.timeout_s is not None:
        _result: list[CommandResult] = []
        _exc: list[BaseException] = []

        def _invoke() -> None:
            try:
                _result.append(
                    _run_checked(transport, ["python3", runner, run.manifest], "invoke runner")
                )
            except BaseException as e:
                _exc.append(e)

        _t = threading.Thread(target=_invoke, daemon=True)
        _t.start()
        _t.join(timeout=job.timeout_s)

        if _t.is_alive():
            # Timed out — best-effort terminate the remote process group (§8.1).
            try:
                pgid_info = json.loads(transport.run(["cat", run.pid]).stdout)
                pgid = int(pgid_info.get("pgid", -1))
                if pgid > 1:
                    terminate_process_group(transport, pgid, grace_s=10.0)
            except Exception:
                pass  # best-effort: PID file may not exist yet or process may have exited
            return AttemptResult(
                number=attempt,
                host=runtime.host,
                status=JobStatus.TIMED_OUT,
                returncode=None,
                duration_s=float(job.timeout_s),
                stdout_log=str(local.stdout_log(attempt)),
                stderr_log=str(local.stderr_log(attempt)),
                failure_kind=FailureKind.TIMEOUT,
                error="job timed out",
            )

        if _exc:
            raise _exc[0]  # re-raise SSH/dispatcher exception from the thread
    else:
        _run_checked(transport, ["python3", runner, run.manifest], "invoke runner")
```

Remove the old `# No timeout here: enforcement + termination are Phase 6 (§8.1).` comment.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_execute_attempt_timeout.py -v`
Expected: both tests pass.

- [ ] **Step 5: Run gate checks**

Run: `uv run ruff check --fix . && uv run ruff check . && uv run mypy`
Expected: all clean. `_result`, `_exc`, `_invoke`, `_t` are local variables inside a conditional block — mypy should type-check them correctly. If it flags `_exc[0]` as `BaseException` (which `raise` accepts), it is fine.

- [ ] **Step 6: Commit**

```bash
git add src/ray_dispatcher/scheduling.py tests/unit/test_execute_attempt_timeout.py
git commit -m "feat: execute_attempt enforces job.timeout_s with thread + terminate_process_group (§8.1)"
```

---

### Task 5: Phase 6c gate

**Files:** none new — full toolchain verification only.

**Interfaces:** none.

- [ ] **Step 1: Run the full suite**

Run: `uv run pytest -q`
Expected: all tests pass (Phases 1–6b + Phase 6c, including all integration tests). Each Ray integration test starts and shuts down its own runtime.

- [ ] **Step 2: Lint + type check**

Run: `uv run ruff check --fix . && uv run ruff check . && uv run mypy`
Expected: ruff auto-sorts then `All checks passed!`; mypy `Success: no issues found`.

Confirm only `backends/ssh_ray.py` imports `ray` (not `scheduling.py`, `base.py`, or any other module).

- [ ] **Step 3: Commit (only if Step 2 made changes)**

```bash
git add -A
git commit -m "chore: phase 6c gate green (ruff + mypy)"
```

---

## Phase 6c self-review

Run before declaring Phase 6c done:

- [ ] `_attempt_task` decorated with exactly `@ray.remote(num_cpus=0, resources={"vm_slot": 1}, max_retries=0)`.
- [ ] `_ActorLeaseHandle` starts a daemon heartbeat thread on `acquire`, stops it before `release`; `heartbeat_interval_s` is configurable and defaults to 5.0.
- [ ] Catch-all: `ray.exceptions.TaskCancelledError` → CANCELLED result; `Exception` → FAILED/INTERNAL result; `write_result_json` called in all three paths (success, cancelled, internal).
- [ ] `SshRayBackend.__init__` accepts `results_dir`; `setup()` populates `self._inv_hosts`; `submit()` creates `JobLayout` and stores the ObjectRef; `status()` derives RUNNING/terminal from `ray.wait`; `cancel()` calls `ray.cancel`; `resolve()` calls `ray.get` with cancellation handling.
- [ ] `execute_attempt` enforces `job.timeout_s`: runner runs in a daemon thread; on timeout, PID file is read, `terminate_process_group` called, TIMEOUT AttemptResult returned; `job.timeout_s is None` takes the direct path (behaviour unchanged).
- [ ] `uv run pytest -q`, `uv run ruff check .`, `uv run mypy` all green; only `ssh_ray.py` imports `ray`; `scheduling.py` remains Ray-free (only gains `import threading`).

**Deliverable:** `SshRayBackend` is fully executable: `submit()` dispatches Ray tasks with `vm_slot: 1` admission control, `max_retries=0`, and live lease heartbeating; `resolve()`/`status()`/`cancel()` work; timeout is enforced in `execute_attempt`; every job terminates as a `JobResult`.

**Residuals carried to Phase 6d:**
- **`Dispatcher`** (§4.5): public lifecycle + `submit`/`as_completed`/`run` batch orchestration + `BatchExistsError` + `raise_on_failure` → `BatchFailedError` + result ordering + context manager.
- **Full teardown** (§10.1): cancel/reconcile outstanding attempts before `ray.shutdown`; `purge=True`; §3.2.6 stale-lock reconciliation at provisioning takeover.
- **`__init__.py` exports:** `Dispatcher`, `SshRayBackend`, and all public types.
- **Status registry** as observable progress (§9.2 `progress` extra): Rich rendering is an optional enhancement.
- **Multipass e2e tests** (§11): Phase 7.
