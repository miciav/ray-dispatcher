# Phase 6b — Ray Backend Lifecycle (runtime + HostLease actor + setup/teardown) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the Ray execution backend's lifecycle: the `ExecutionBackend` ABC, the `HostLease` Ray actor (decorating `LeaseService`), and `SshRayBackend.setup`/`teardown` — exclusive local Ray runtime (§3.2), the `vm_slot` custom resource, per-host `HostRuntime` assembly, and clean teardown (§10). Job submission/execution is Phase 6c.

**Architecture:** `SshRayBackend` runs entirely on the driver process here — it provisions the inventory (Phase 3 `provision`), then starts an exclusive local Ray runtime and creates one `HostLease` actor (`ray.remote(num_cpus=0)(LeaseService)`) seeded with `{host: slots}` for the healthy hosts. A thin synchronous `_ActorLeaseHandle` adapts the async actor to the `LeaseHandle` protocol (via `ray.get`) for the Phase 6c attempt task. No transports cross into Ray here (provisioning + the `$HOME` probe are driver-side), so this phase is testable with local Ray + an in-memory `FakeTransport` factory.

**Tech Stack:** Python 3.10+, `ray>=2.40,<3` (already a declared dependency); existing `ray_dispatcher.scheduling` (`LeaseService`, `HostRuntime`, `secret_env_map`, `LeaseHandle`, `Lease`), `ray_dispatcher.provisioning` (`provision`, `ProvisioningOutcome`, `RemoteLayout`), `ray_dispatcher.digests` (`runner_digest`), `ray_dispatcher` (`remote_runner` module for the runner path), `ray_dispatcher.models`, `ray_dispatcher.errors`. Unit tests use `pytest`; the Ray tests live in `tests/integration/` and start an isolated local Ray runtime.

## Global Constraints

- Python floor 3.10; `from __future__ import annotations` at the top of every module.
- mypy strict (`files=["src"]`) and ruff (`E/F/I/UP/B`, line-length 100) must pass with zero errors.
- **§3.2 exclusive Ray runtime (binding):**
  1. `setup()` raises `RayRuntimeConflictError` if `ray.is_initialized()` is already true, and MUST NOT call `ray.shutdown()` in that case (it does not own that runtime).
  2. Provision the inventory first, freeze the healthy host set, THEN start Ray with `address="local"`, a unique `namespace`, and `resources={"vm_slot": sum(host.slots for healthy hosts)}`.
  3. The `HostLease` actor declares `num_cpus=0` explicitly (`ray.remote(num_cpus=0)(LeaseService)`). (The attempt task's `num_cpus=0`/`resources={"vm_slot": 1}`/`max_retries=0` is Phase 6c.)
  4. Only the backend that successfully started Ray may call `ray.shutdown()` (track an `_owns_runtime` flag).
- **§3.3 backend-neutral handles:** Ray types are never exposed by the public API; the ABC signatures are exact (below).
- **§10 teardown (the slice built here):** stop the lease actor, release the remote session locks (`ProvisioningOutcome.release_all()`), and call `ray.shutdown()` only for the owned runtime. (Cancel/reconcile of outstanding attempts, purge, and stale-lock recovery are Phase 6c/6d.)
- Data types already defined (do NOT redefine): `Inventory`, `Project`, `Job`, `JobHandle`, `JobStatus`, `JobResult`, `ProvisioningReport`, `RetryPolicy` (`models.py`); `RayRuntimeConflictError`, `NoHealthyHostsError`, `ProvisioningError` (`errors.py`); `LeaseService`, `HostRuntime`, `secret_env_map`, `LeaseHandle`, `Lease` (`scheduling.py`); `provision`, `ProvisioningOutcome`, `RemoteLayout` (`provisioning.py`); `runner_digest` (`digests.py`).
- §5 module layout: only `SshRayBackend`/`backends/ssh_ray.py` imports Ray-facing implementation types. `backends/base.py` is Ray-free.

**Deferred to Phase 6c/6d (do NOT build here):** `submit`/`resolve`/`status`/`cancel`; the `@ray.remote` attempt task wrapping `run_job`; the concurrent lease heartbeat (§8.2); timeout/termination (§8.1); §3.2.6 stale-lock reconciliation; `purge`; cancel/reconcile of outstanding attempts on teardown; the `Dispatcher` and its batch orchestration; status registry (§9.2); `__init__.py` exports of the backend/dispatcher.

---

### Task 1: `ExecutionBackend` ABC

**Files:**
- Create: `src/ray_dispatcher/backends/__init__.py`
- Create: `src/ray_dispatcher/backends/base.py`
- Test: `tests/unit/test_backend_base.py`

**Interfaces:**
- Consumes: `models.Inventory`/`Project`/`Job`/`JobHandle`/`JobStatus`/`JobResult`/`ProvisioningReport`.
- Produces: `ExecutionBackend(ABC)` with abstract methods exactly per spec §3.3: `setup(inventory, project) -> ProvisioningReport`, `submit(batch_id, job) -> JobHandle`, `status(handle) -> JobStatus`, `cancel(handle) -> None`, `resolve(handle) -> JobResult`, `teardown(*, purge=False) -> None`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_backend_base.py`:

```python
import inspect

import pytest

from ray_dispatcher.backends.base import ExecutionBackend


def test_cannot_instantiate_abstract_backend():
    with pytest.raises(TypeError):
        ExecutionBackend()  # abstract — has unimplemented abstract methods


def test_declares_the_spec_3_3_methods():
    expected = {"setup", "submit", "status", "cancel", "resolve", "teardown"}
    assert expected <= set(ExecutionBackend.__abstractmethods__)


def test_setup_signature_takes_inventory_and_project():
    sig = inspect.signature(ExecutionBackend.setup)
    assert list(sig.parameters)[1:] == ["inventory", "project"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_backend_base.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ray_dispatcher.backends'`.

- [ ] **Step 3: Write the implementation**

Create `src/ray_dispatcher/backends/__init__.py` (empty package marker):

```python
"""Execution backends for ray_dispatcher."""
```

Create `src/ray_dispatcher/backends/base.py`:

```python
"""The backend-neutral execution interface (spec §3.3).

A backend maps opaque JobHandles to its own execution mechanism; Ray types never
cross this boundary, so a future non-Ray backend can satisfy the same contract.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import Inventory, Job, JobHandle, JobResult, JobStatus, Project, ProvisioningReport


class ExecutionBackend(ABC):
    @abstractmethod
    def setup(self, inventory: Inventory, project: Project) -> ProvisioningReport: ...

    @abstractmethod
    def submit(self, batch_id: str, job: Job) -> JobHandle: ...

    @abstractmethod
    def status(self, handle: JobHandle) -> JobStatus: ...

    @abstractmethod
    def cancel(self, handle: JobHandle) -> None: ...

    @abstractmethod
    def resolve(self, handle: JobHandle) -> JobResult: ...

    @abstractmethod
    def teardown(self, *, purge: bool = False) -> None: ...
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_backend_base.py -v`
Expected: all three cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/backends/__init__.py src/ray_dispatcher/backends/base.py tests/unit/test_backend_base.py
git commit -m "feat: add ExecutionBackend ABC (spec §3.3)"
```

---

### Task 2: `HostLease` actor + `_ActorLeaseHandle` adapter

**Files:**
- Create: `src/ray_dispatcher/backends/ssh_ray.py`
- Test: `tests/integration/test_host_lease_actor.py`
- Create (if absent): `tests/integration/__init__.py`

**Interfaces:**
- Consumes: `scheduling.LeaseService`/`Lease`/`LeaseHandle`; `ray`.
- Produces:
  - `HostLease = ray.remote(num_cpus=0)(LeaseService)` — the lease actor class (an async Ray actor; `LeaseService`'s async methods become actor methods).
  - `_ActorLeaseHandle(actor)` — a synchronous `LeaseHandle`: `acquire(attempt_id, *, exclude=()) -> Lease` does `ray.get(actor.acquire.remote(attempt_id, exclude=tuple(exclude)))`; `release(token) -> None` does `ray.get(actor.release.remote(token))`.

- [ ] **Step 1: Write the failing test**

`tests/integration/__init__.py`: create empty if it does not exist.

`tests/integration/test_host_lease_actor.py`:

```python
import ray

from ray_dispatcher.backends.ssh_ray import HostLease, _ActorLeaseHandle


def test_actor_lease_acquire_release_roundtrip():
    ray.init(address="local", namespace="test-hostlease", num_cpus=2)
    try:
        actor = HostLease.remote({"a": 2})  # 2 slots on host "a"
        handle = _ActorLeaseHandle(actor)
        l1 = handle.acquire("job/1")
        l2 = handle.acquire("job/2")
        assert {l1.host, l2.host} == {"a"} and l1.token != l2.token
        # both slots taken; free one and re-acquire
        handle.release(l1.token)
        l3 = handle.acquire("job/3")
        assert l3.host == "a"
    finally:
        ray.shutdown()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_host_lease_actor.py -v`
Expected: FAIL with `ImportError: cannot import name 'HostLease'` (the module does not exist yet).

- [ ] **Step 3: Write the implementation**

Create `src/ray_dispatcher/backends/ssh_ray.py`:

```python
"""Exclusive local-Ray execution backend (spec §3.2, §3.3).

This is the only module that imports Ray. The HostLease actor wraps the Ray-free
LeaseService; SshRayBackend owns one local Ray runtime, started after provisioning
and shut down at teardown.
"""

from __future__ import annotations

from collections.abc import Iterable

import ray

from ..scheduling import Lease, LeaseService

# The async lease state machine, run as a Ray actor that holds no host CPU (§3.2.3).
HostLease = ray.remote(num_cpus=0)(LeaseService)


class _ActorLeaseHandle:
    """Synchronous LeaseHandle over the async HostLease actor (used by the 6c task)."""

    def __init__(self, actor: ray.actor.ActorHandle) -> None:
        self._actor = actor

    def acquire(self, attempt_id: str, *, exclude: Iterable[str] = ()) -> Lease:
        return ray.get(self._actor.acquire.remote(attempt_id, exclude=tuple(exclude)))

    def release(self, token: str) -> None:
        ray.get(self._actor.release.remote(token))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_host_lease_actor.py -v`
Expected: PASS (starts a local Ray runtime, exercises the actor, shuts down).

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/backends/ssh_ray.py tests/integration/test_host_lease_actor.py tests/integration/__init__.py
git commit -m "feat: add HostLease Ray actor + sync lease handle adapter (§3.2.3)"
```

---

### Task 3: `SshRayBackend` skeleton + setup() exclusive-runtime guard

**Files:**
- Modify: `src/ray_dispatcher/backends/ssh_ray.py` (add `SshRayBackend` with `__init__` + `setup` guard + stubs)
- Test: `tests/integration/test_backend_exclusive_runtime.py`

**Interfaces:**
- Consumes: `backends.base.ExecutionBackend`; `models.Inventory`/`Project`/`Job`/`JobHandle`/`JobStatus`/`JobResult`/`ProvisioningReport`/`RetryPolicy`; `errors.RayRuntimeConflictError`; `ssh`-building transport factory (`provisioning._default_transport`); `ray`.
- Produces:
  - `SshRayBackend(*, runner_path: str | None = None, transport_factory=None, retry_policy: RetryPolicy = RetryPolicy(), min_disk_mb: int = 500)` implementing `ExecutionBackend`.
  - `setup(inventory, project)` begins by raising `RayRuntimeConflictError` when `ray.is_initialized()` (no `ray.shutdown()`). The full body lands in Task 4; `submit`/`status`/`cancel`/`resolve`/`teardown` are stubs raising `NotImplementedError` until later tasks.

- [ ] **Step 1: Write the failing test**

`tests/integration/test_backend_exclusive_runtime.py`:

```python
import ray

from ray_dispatcher.backends.ssh_ray import SshRayBackend
from ray_dispatcher.errors import RayRuntimeConflictError
from ray_dispatcher.models import Inventory, Project, RemoteHost


def _project():
    return Project(path="/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25")


def test_setup_rejects_preinitialized_ray_without_shutting_it_down():
    ray.init(address="local", namespace="preexisting", num_cpus=1)
    try:
        backend = SshRayBackend()
        inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu"),))
        try:
            backend.setup(inv, _project())
            raise AssertionError("expected RayRuntimeConflictError")
        except RayRuntimeConflictError:
            pass
        assert ray.is_initialized()  # the caller's runtime must NOT be shut down
    finally:
        ray.shutdown()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_backend_exclusive_runtime.py -v`
Expected: FAIL with `ImportError: cannot import name 'SshRayBackend'`.

- [ ] **Step 3: Write the implementation**

Extend the imports of `ssh_ray.py`:

```python
from ..digests import runner_digest
from ..errors import RayRuntimeConflictError
from ..models import (
    Inventory,
    Job,
    JobHandle,
    JobResult,
    JobStatus,
    Project,
    ProvisioningReport,
    RetryPolicy,
)
from ..provisioning import ProvisioningOutcome, RemoteLayout, _default_transport
from ..scheduling import HostRuntime, secret_env_map
from ..ssh import Transport
from .base import ExecutionBackend
```

(merge the new `from ..scheduling import ...` names into the existing scheduling import line: `from ..scheduling import HostRuntime, Lease, LeaseService, secret_env_map`.) Add `from collections.abc import Callable` to the existing `from collections.abc import Iterable` line: `from collections.abc import Callable, Iterable`. Append:

```python
class SshRayBackend(ExecutionBackend):
    """Owns one exclusive local Ray runtime and a HostLease actor (spec §3.2)."""

    def __init__(
        self,
        *,
        runner_path: str | None = None,
        transport_factory: Callable[..., Transport] | None = None,
        retry_policy: RetryPolicy = RetryPolicy(),
        min_disk_mb: int = 500,
    ) -> None:
        from .. import remote_runner

        self._runner_path = runner_path or remote_runner.__file__
        self._transport_factory = transport_factory or _default_transport
        self._retry_policy = retry_policy
        self._min_disk_mb = min_disk_mb
        self._owns_runtime = False
        self._actor: ray.actor.ActorHandle | None = None
        self._outcome: ProvisioningOutcome | None = None
        self._runtimes: dict[str, HostRuntime] = {}

    def setup(self, inventory: Inventory, project: Project) -> ProvisioningReport:
        if ray.is_initialized():
            raise RayRuntimeConflictError(
                "a Ray runtime is already initialized; ray_dispatcher owns its own "
                "local runtime and will not attach to an external one (§3.2)"
            )
        raise NotImplementedError  # full body in Task 4

    def submit(self, batch_id: str, job: Job) -> JobHandle:
        raise NotImplementedError  # Phase 6c

    def status(self, handle: JobHandle) -> JobStatus:
        raise NotImplementedError  # Phase 6c

    def cancel(self, handle: JobHandle) -> None:
        raise NotImplementedError  # Phase 6c

    def resolve(self, handle: JobHandle) -> JobResult:
        raise NotImplementedError  # Phase 6c

    def teardown(self, *, purge: bool = False) -> None:
        raise NotImplementedError  # Task 5
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_backend_exclusive_runtime.py -v`
Expected: PASS (`setup` raises `RayRuntimeConflictError` before reaching the `NotImplementedError`; the pre-existing runtime stays up).

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/backends/ssh_ray.py tests/integration/test_backend_exclusive_runtime.py
git commit -m "feat: SshRayBackend skeleton + exclusive-runtime guard (§3.2.1)"
```

---

### Task 4: `setup()` — provision, start Ray, create actor, build runtimes

**Files:**
- Modify: `src/ray_dispatcher/backends/ssh_ray.py` (full `setup` body + `_resolve_home` + `_build_runtime` helpers)
- Test: `tests/integration/test_backend_setup.py`

**Interfaces:**
- Consumes: `provisioning.provision(inventory, project, *, runner_path, transport_factory, ...) -> ProvisioningOutcome` (its `.report.hosts` carry `host`/`succeeded`/`environment_digest`; `.sessions` hold the live locks); `digests.runner_digest(path) -> str`; `scheduling.HostRuntime`/`secret_env_map`; `RemoteLayout(home, project_id)`.
- Produces: `setup` returns the `ProvisioningReport`; on success it has started Ray (`_owns_runtime=True`), created the `HostLease` actor seeded with `{host: slots}` for healthy hosts, and stored a `HostRuntime` per healthy host in `self._runtimes` (keyed by `host.host`).

- [ ] **Step 1: Write the failing test**

`tests/integration/test_backend_setup.py`:

Provisioning is already exhaustively tested in Phase 3b, so we monkeypatch `provision`
to return a canned `ProvisioningOutcome` and focus this test on setup's OWN logic
(runtime bring-up, `vm_slot`, the actor, and `HostRuntime` building). Only the `$HOME`
probe needs a (trivial) fake transport.

```python
import pytest
import ray

from ray_dispatcher.backends import ssh_ray
from ray_dispatcher.backends.ssh_ray import SshRayBackend
from ray_dispatcher.errors import NoHealthyHostsError
from ray_dispatcher.models import (
    HostProvisioningResult,
    Inventory,
    Project,
    ProvisioningReport,
    RemoteHost,
)
from ray_dispatcher.provisioning import ProvisioningOutcome
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _project():
    return Project(path="/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25")


def _home_transport(host):
    def results(argv):
        if 'printf %s "$HOME"' in " ".join(argv):
            return CommandResult(0, "/home/ubuntu", "", 0.0)
        return CommandResult(0, "", "", 0.0)
    return FakeTransport(run_results=results)


def _canned_outcome(*host_names):
    report = ProvisioningReport(tuple(
        HostProvisioningResult(h, True, "src123", "env123") for h in host_names
    ))
    return ProvisioningOutcome(report, sessions={})  # no live locks in this test


def test_setup_starts_runtime_with_vm_slot_sum_and_builds_runtimes(monkeypatch):
    inv = Inventory((
        RemoteHost("10.0.0.1", user="ubuntu", slots=2),
        RemoteHost("10.0.0.2", user="ubuntu", slots=3),
    ))
    monkeypatch.setattr(ssh_ray, "provision",
                        lambda *a, **k: _canned_outcome("10.0.0.1", "10.0.0.2"))
    backend = SshRayBackend(transport_factory=_home_transport)
    try:
        report = backend.setup(inv, _project())
        assert all(h.succeeded for h in report.hosts)
        assert ray.is_initialized()
        assert ray.cluster_resources().get("vm_slot") == 5.0   # 2 + 3 (§3.2.2)
        assert set(backend._runtimes) == {"10.0.0.1", "10.0.0.2"}
        rt = backend._runtimes["10.0.0.1"]
        assert rt.layout.source == "/home/ubuntu/.ray_dispatcher/projects/dfaas/source"
        assert rt.environment_digest == "env123" and rt.runner_digest
    finally:
        backend._teardown_runtime_for_test()   # real teardown() arrives in Task 5


def test_setup_propagates_no_healthy_hosts_without_starting_ray(monkeypatch):
    def _raise(*a, **k):
        raise NoHealthyHostsError("no host provisioned successfully")

    monkeypatch.setattr(ssh_ray, "provision", _raise)
    backend = SshRayBackend(transport_factory=_home_transport)
    with pytest.raises(NoHealthyHostsError):
        backend.setup(Inventory((RemoteHost("10.0.0.9", user="ubuntu"),)), _project())
    assert not ray.is_initialized()   # Ray never started when provisioning yields no host
```

`_teardown_runtime_for_test` is a tiny test-only release helper added in this task's Step 3 (the real `teardown()` lands in Task 5, which deletes the helper and switches this `finally` to `backend.teardown()`). The `test_setup_propagates...` case never starts Ray, so it needs no cleanup.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_backend_setup.py -v`
Expected: FAIL with `NotImplementedError` (from the Task 3 stub) on the first test.

- [ ] **Step 3: Write the implementation**

Replace the `setup` body (the `if ray.is_initialized(): ...` guard stays first) and add the helpers. Add `import secrets` to the stdlib import group (before `import ray` — ruff orders plain `import` stdlib before third-party). Add `provision` to the provisioning import line: `from ..provisioning import ProvisioningOutcome, RemoteLayout, _default_transport, provision`.

```python
    def setup(self, inventory: Inventory, project: Project) -> ProvisioningReport:
        if ray.is_initialized():
            raise RayRuntimeConflictError(
                "a Ray runtime is already initialized; ray_dispatcher owns its own "
                "local runtime and will not attach to an external one (§3.2)"
            )
        # Provision first; provision() raises NoHealthyHostsError if none succeed,
        # so Ray is never started for an empty healthy set (§3.2.2).
        outcome = provision(
            inventory, project,
            runner_path=self._runner_path, transport_factory=self._transport_factory,
            min_disk_mb=self._min_disk_mb,
        )
        self._outcome = outcome
        runner_dig = runner_digest(self._runner_path)
        healthy = {h.host: h for h in outcome.report.hosts if h.succeeded}
        inv_slots = {h.host: h.slots for h in inventory.hosts}
        slots = {name: inv_slots[name] for name in healthy}  # healthy host -> its slots
        self._runtimes = {
            host_name: self._build_runtime(inventory, project, result, runner_dig)
            for host_name, result in healthy.items()
        }
        ray.init(
            address="local",
            namespace=f"ray-dispatcher-{secrets.token_hex(8)}",  # unique per session (§3.2.2)
            resources={"vm_slot": float(sum(slots.values()))},
        )
        self._owns_runtime = True
        self._actor = HostLease.remote(slots)
        return outcome.report

    def _build_runtime(
        self, inventory: Inventory, project: Project, result: object, runner_dig: str
    ) -> HostRuntime:
        host_name = result.host  # type: ignore[attr-defined]
        host = next(h for h in inventory.hosts if h.host == host_name)
        transport = self._transport_factory(host)
        home = transport.run(["sh", "-c", 'printf %s "$HOME"']).stdout.strip()
        layout = RemoteLayout(home, project.project_id)
        env_dig = result.environment_digest  # type: ignore[attr-defined]
        assert env_dig is not None  # healthy hosts always have an environment digest
        return HostRuntime(
            host=host_name,
            layout=layout,
            environment_digest=env_dig,
            runner_digest=runner_dig,
            project_path=project.path,
            secret_env=secret_env_map(project, layout),
        )

    def _teardown_runtime_for_test(self) -> None:
        """Minimal runtime release for tests written before Task 5's teardown."""
        if self._actor is not None:
            ray.kill(self._actor)
            self._actor = None
        if self._outcome is not None:
            self._outcome.release_all()
        if self._owns_runtime:
            ray.shutdown()
            self._owns_runtime = False
```

(Note: `_build_runtime` takes `result` as `object` with two `# type: ignore[attr-defined]` accesses to avoid importing `HostProvisioningResult` solely for typing; if you prefer, import `HostProvisioningResult` from `..models` and type `result: HostProvisioningResult` and drop the ignores — either is acceptable as long as mypy passes.)

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_backend_setup.py -v`
Expected: both cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/backends/ssh_ray.py tests/integration/test_backend_setup.py
git commit -m "feat: SshRayBackend.setup — provision, start Ray, vm_slot, HostLease, runtimes (§3.2.2)"
```

---

### Task 5: `teardown()` (§10 slice)

**Files:**
- Modify: `src/ray_dispatcher/backends/ssh_ray.py` (real `teardown`; remove `_teardown_runtime_for_test`)
- Test: `tests/integration/test_backend_teardown.py`

**Interfaces:**
- Consumes: the state set by `setup` (`_actor`, `_outcome`, `_owns_runtime`).
- Produces: `teardown(*, purge=False)` — kill the `HostLease` actor, release the remote session locks (`outcome.release_all()`), and `ray.shutdown()` only if `_owns_runtime`; idempotent and safe to call without a prior `setup`. (`purge=True` and cancel/reconcile of outstanding attempts are Phase 6c/6d — accept the flag, ignore it for now, with a `# ponytail:` note.) Replace the test-only `_teardown_runtime_for_test` calls in `test_backend_setup.py` with `teardown()`.

- [ ] **Step 1: Write the failing test**

`tests/integration/test_backend_teardown.py`:

```python
import ray

from ray_dispatcher.backends import ssh_ray
from ray_dispatcher.backends.ssh_ray import SshRayBackend
from ray_dispatcher.models import (
    HostProvisioningResult,
    Inventory,
    Project,
    ProvisioningReport,
    RemoteHost,
)
from ray_dispatcher.provisioning import ProvisioningOutcome
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _project():
    return Project(path="/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25")


def _home_transport(host):
    def results(argv):
        if 'printf %s "$HOME"' in " ".join(argv):
            return CommandResult(0, "/home/ubuntu", "", 0.0)
        return CommandResult(0, "", "", 0.0)
    return FakeTransport(run_results=results)


class _Recorder:
    """Stands in for both the session lock and its heartbeat thread.

    `ProvisioningOutcome.release_all()` calls `hb.stop()` then `lock.release()`;
    recording both lets the test prove release_all actually iterated, not just
    that the dict was reassigned to {}.
    """

    def __init__(self):
        self.calls = []

    def stop(self):
        self.calls.append("stop")

    def release(self):
        self.calls.append("release")


def _canned_outcome(host_name, session):
    report = ProvisioningReport((HostProvisioningResult(host_name, True, "src123", "env123"),))
    return ProvisioningOutcome(report, sessions={host_name: (session, session)})


def test_teardown_shuts_down_owned_runtime_and_releases_locks(monkeypatch):
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=1),))
    rec = _Recorder()
    monkeypatch.setattr(ssh_ray, "provision",
                        lambda *a, **k: _canned_outcome("10.0.0.1", rec))
    backend = SshRayBackend(transport_factory=_home_transport)
    backend.setup(inv, _project())
    assert ray.is_initialized()
    backend.teardown()
    assert not ray.is_initialized()         # owned runtime shut down (§10.5)
    assert rec.calls == ["stop", "release"]  # release_all() ran: hb stopped, lock released (§10.4)
    assert backend._outcome.sessions == {}   # sessions cleared


def test_teardown_is_safe_without_setup():
    backend = SshRayBackend(transport_factory=_home_transport)
    backend.teardown()  # no runtime owned -> no error, no ray.shutdown of a foreign runtime
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_backend_teardown.py -v`
Expected: FAIL — the first case hits the Task 3 `NotImplementedError` in `teardown`.

- [ ] **Step 3: Write the implementation**

Replace the `teardown` stub and delete `_teardown_runtime_for_test`:

```python
    def teardown(self, *, purge: bool = False) -> None:
        # ponytail: purge + cancel/reconcile of outstanding attempts land in 6c/6d;
        # this slice does the runtime + lock release (§10.2,4,5).
        if self._actor is not None:
            ray.kill(self._actor)
            self._actor = None
        if self._outcome is not None:
            self._outcome.release_all()
        if self._owns_runtime:
            ray.shutdown()
            self._owns_runtime = False
```

Then edit `tests/integration/test_backend_setup.py`: replace both `backend._teardown_runtime_for_test()` calls with `backend.teardown()`.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_backend_teardown.py tests/integration/test_backend_setup.py -v`
Expected: all cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/backends/ssh_ray.py tests/integration/test_backend_teardown.py tests/integration/test_backend_setup.py
git commit -m "feat: SshRayBackend.teardown — kill actor, release locks, shutdown owned runtime (§10)"
```

---

### Task 6: Phase 6b gate

**Files:**
- (no new source) — final full-toolchain verification.

**Interfaces:** none.

- [ ] **Step 1: Run the full suite**

Run: `uv run pytest -q`
Expected: all tests `passed` (Phases 1–6a + Phase 6b, including the `tests/integration` Ray tests). Each Ray test starts and shuts down its own runtime, so they do not leak a runtime between tests.

- [ ] **Step 2: Lint + type check**

Run: `uv run ruff check --fix . && uv run ruff check . && uv run mypy`
Expected: ruff auto-sorts then `All checks passed!`; mypy `Success: no issues found`.

If mypy flags `ray.actor.ActorHandle` or the `HostLease = ray.remote(...)` assignment (Ray's stubs are partial), fix minimally — prefer a precise local type or a single targeted `# type: ignore[...]` with the error code, never a config relaxation. Confirm only `backends/ssh_ray.py` imports `ray` (not `base.py`, not `scheduling.py`).

- [ ] **Step 3: Commit (only if Step 2 made changes)**

```bash
git add -A
git commit -m "chore: phase 6b gate green (ruff + mypy)"
```

---

## Phase 6b self-review

Run before declaring Phase 6b done:

- [ ] `ExecutionBackend` ABC declares exactly the §3.3 methods; `base.py` does not import `ray`.
- [ ] `HostLease = ray.remote(num_cpus=0)(LeaseService)`; `_ActorLeaseHandle` satisfies `LeaseHandle` via `ray.get` and round-trips acquire/release against a live actor.
- [ ] `setup` raises `RayRuntimeConflictError` first when `ray.is_initialized()` and never calls `ray.shutdown()` in that case; provisions before starting Ray; starts Ray `address="local"` with a unique namespace and `vm_slot = sum(healthy slots)`; builds a `HostRuntime` per healthy host (home probed once); `provision`'s `NoHealthyHostsError` propagates with Ray un-started.
- [ ] `teardown` kills the actor, releases session locks, and shuts down only the owned runtime; idempotent without `setup`.
- [ ] `_owns_runtime` gates `ray.shutdown()` so a foreign runtime is never shut down (§3.2.4).
- [ ] `uv run pytest -q`, `uv run ruff check .`, `uv run mypy` all green; only `ssh_ray.py` imports `ray`.

**Deliverable:** `backends/base.py` (`ExecutionBackend`), `backends/ssh_ray.py` (`HostLease`, `_ActorLeaseHandle`, `SshRayBackend` with `setup`/`teardown`). The Ray runtime comes up exclusively, the lease actor is live, and teardown is clean — the foundation the Phase 6c attempt task and submission build on.

**Residuals carried to Phase 6c/6d (documented):**
- **Phase 6c — submission + execution:** `submit(batch_id, job)` launches a `@ray.remote(num_cpus=0, resources={"vm_slot": 1}, max_retries=0)` task that builds its transport inside the task (transports are not Ray-serializable), adapts the actor via `_ActorLeaseHandle`, and calls `run_job`; `resolve`/`status`/`cancel` map `JobHandle`↔`ObjectRef`; the concurrent heartbeat (§8.2) and timeout/termination (§8.1, `TIMEOUT`); a catch-all so every job yields a `JobResult` (e.g. malformed remote `result.json` → `INTERNAL`, per the 6a residual); the `result.json` write via `write_result_json`. Integration tests: concurrency reaches `sum(slots)`, per-host ≤ slots, `max_retries=0` (no auto-retry), completion order.
- **Phase 6c/6d — teardown completion:** cancel/reconcile of outstanding attempts (§10.1) and `purge=True` (§10 purge of this project's inactive remote state); §3.2.6 stale-lock reconciliation at provisioning takeover.
- **Phase 6d — public surface:** `Dispatcher` (§4.5 lifecycle + batch orchestration, `BatchExistsError`, `raise_on_failure`→`BatchFailedError`, result ordering, context manager), status registry (§9.2), and `__init__.py` exports of `Dispatcher`/`SshRayBackend`.
