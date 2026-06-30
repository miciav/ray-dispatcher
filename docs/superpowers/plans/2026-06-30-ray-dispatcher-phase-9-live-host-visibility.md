# Phase 9: Live Host Visibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose, through the public API, which host a currently-`RUNNING` job is leased to — a snapshot dict the `DFaaSOptimizer` `remote_experiments` TUI will poll once per tick for its per-VM progress panel.

**Architecture:** Add one read-only method at each layer that already touches lease state — `LeasePool` (sync, pure) → `LeaseService` (async wrapper) → `ExecutionBackend`/`SshRayBackend` (Ray actor call) → `Dispatcher` (public delegate) — reusing the `Lease.attempt_id`/`Lease.host` fields the lease pool already tracks for every in-flight attempt. No new state, no new actor, no new Ray remote calls beyond one extra method on the existing `HostLease` actor.

**Tech Stack:** Python 3.10, Ray (local runtime, no Multipass needed for these tests), pytest, asyncio (stdlib).

## Global Constraints

- Indentation: 4 spaces (this repo's existing style — do not use 2-space, that's the sibling `DFaaSOptimizer` repo's convention).
- Line length: 100 (`pyproject.toml` `[tool.ruff]`).
- Target Python: 3.10 (`target-version = "py310"`).
- Follow the existing one-behavior-per-test-file convention in `tests/unit/` (e.g. `test_lease_pool_quarantined_hosts.py`).
- No new dependencies.
- Reference design: [docs/superpowers/specs/2026-06-30-live-host-visibility-design.md](../specs/2026-06-30-live-host-visibility-design.md).

---

### Task 1: `LeasePool.current_hosts()`

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (the `LeasePool` class, after `quarantined_hosts`, around line 178)
- Test: `tests/unit/test_lease_pool_current_hosts.py`

**Interfaces:**
- Produces: `LeasePool.current_hosts(self) -> dict[str, str]` — `{attempt_id: host}` for every lease currently held (acquired and not yet released/quarantined-away).

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_lease_pool_current_hosts.py
from ray_dispatcher.scheduling import LeasePool


class Clock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t
    def __call__(self) -> float:
        return self.t


def _tokens():
    n = 0
    def factory() -> str:
        nonlocal n
        t = f"tok{n}"
        n += 1
        return t
    return factory


def test_current_hosts_empty_when_no_leases():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    assert pool.current_hosts() == {}


def test_current_hosts_reports_attempt_id_to_host():
    pool = LeasePool({"a": 1, "b": 1}, now=Clock(), token_factory=_tokens())
    pool.acquire("job-1")
    pool.acquire("job-2")
    assert pool.current_hosts() == {"job-1": "a", "job-2": "b"}


def test_current_hosts_drops_entry_after_release():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    lease = pool.acquire("job-1")
    pool.release(lease.token)
    assert pool.current_hosts() == {}


def test_current_hosts_drops_entry_after_quarantine():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    pool.acquire("job-1")
    pool.quarantine("a")
    assert pool.current_hosts() == {}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/micheleciavotta/Downloads/ray-dispatcher && uv run pytest tests/unit/test_lease_pool_current_hosts.py -v`
Expected: FAIL with `AttributeError: 'LeasePool' object has no attribute 'current_hosts'`

- [ ] **Step 3: Implement**

In `src/ray_dispatcher/scheduling.py`, add to `LeasePool` right after `quarantined_hosts`:

```python
    def current_hosts(self) -> dict[str, str]:
        return {ls.attempt_id: ls.host for ls in self._leases.values()}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/micheleciavotta/Downloads/ray-dispatcher && uv run pytest tests/unit/test_lease_pool_current_hosts.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
cd /Users/micheleciavotta/Downloads/ray-dispatcher
git add src/ray_dispatcher/scheduling.py tests/unit/test_lease_pool_current_hosts.py
git commit -m "feat: add LeasePool.current_hosts() live lease snapshot"
```

---

### Task 2: `LeaseService.current_hosts()`

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (the `LeaseService` class, after `quarantined_hosts`)
- Test: `tests/unit/test_lease_service_current_hosts.py`

**Interfaces:**
- Consumes: `LeasePool.current_hosts() -> dict[str, str]` (Task 1)
- Produces: `LeaseService.current_hosts(self) -> Coroutine[..., dict[str, str]]` (async method, called as `await svc.current_hosts()`)

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_lease_service_current_hosts.py
import asyncio

from ray_dispatcher.scheduling import LeaseService


class Clock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t
    def __call__(self) -> float:
        return self.t


def _tokens():
    n = 0
    def factory() -> str:
        nonlocal n
        t = f"tok{n}"
        n += 1
        return t
    return factory


def test_current_hosts_delegates_to_pool():
    async def scenario():
        svc = LeaseService({"a": 1}, lease_ttl_s=60.0, now=Clock(), token_factory=_tokens())
        assert await svc.current_hosts() == {}
        await svc.acquire("job-1")
        assert await svc.current_hosts() == {"job-1": "a"}

    asyncio.run(scenario())
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/micheleciavotta/Downloads/ray-dispatcher && uv run pytest tests/unit/test_lease_service_current_hosts.py -v`
Expected: FAIL with `AttributeError: 'LeaseService' object has no attribute 'current_hosts'`

- [ ] **Step 3: Implement**

In `src/ray_dispatcher/scheduling.py`, add to `LeaseService` right after `quarantined_hosts`:

```python
    async def current_hosts(self) -> dict[str, str]:
        async with self._cond:
            return self._pool.current_hosts()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/micheleciavotta/Downloads/ray-dispatcher && uv run pytest tests/unit/test_lease_service_current_hosts.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/micheleciavotta/Downloads/ray-dispatcher
git add src/ray_dispatcher/scheduling.py tests/unit/test_lease_service_current_hosts.py
git commit -m "feat: add LeaseService.current_hosts() async wrapper"
```

---

### Task 3: `ExecutionBackend.running_hosts()` abstract method

**Files:**
- Modify: `src/ray_dispatcher/backends/base.py`
- Modify: `tests/unit/test_backend_base.py`

**Interfaces:**
- Produces: `ExecutionBackend.running_hosts(self) -> dict[str, str]` (abstract; every backend must implement it)

- [ ] **Step 1: Write the failing test**

In `tests/unit/test_backend_base.py`, change the `expected` set in `test_declares_the_spec_3_3_methods`:

```python
def test_declares_the_spec_3_3_methods():
    expected = {"setup", "submit", "status", "cancel", "resolve", "teardown", "running_hosts"}
    assert expected <= set(ExecutionBackend.__abstractmethods__)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/micheleciavotta/Downloads/ray-dispatcher && uv run pytest tests/unit/test_backend_base.py -v`
Expected: FAIL — `"running_hosts"` not in `ExecutionBackend.__abstractmethods__`

- [ ] **Step 3: Implement**

In `src/ray_dispatcher/backends/base.py`, add after `resolve`:

```python
    @abstractmethod
    def running_hosts(self) -> dict[str, str]: ...
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/micheleciavotta/Downloads/ray-dispatcher && uv run pytest tests/unit/test_backend_base.py -v`
Expected: PASS — but this will now break `SshRayBackend` instantiation (it no longer satisfies the ABC). That's expected; Task 4 fixes it. Run the full unit suite to confirm the breakage is contained:

Run: `cd /Users/micheleciavotta/Downloads/ray-dispatcher && uv run pytest tests/unit -v 2>&1 | tail -30`
Expected: failures only in tests that instantiate `SshRayBackend` (e.g. `test_dispatcher.py`'s `_mock_backend` uses `MagicMock(spec=ExecutionBackend)`, which does NOT fail — `MagicMock` auto-stubs new abstract methods. Real `SshRayBackend()` instantiation, if any unit test does that directly, will fail with `TypeError: Can't instantiate abstract class`.)

- [ ] **Step 5: Commit**

```bash
cd /Users/micheleciavotta/Downloads/ray-dispatcher
git add src/ray_dispatcher/backends/base.py tests/unit/test_backend_base.py
git commit -m "feat: add running_hosts() to the ExecutionBackend contract"
```

---

### Task 4: `SshRayBackend.running_hosts()`

**Files:**
- Modify: `src/ray_dispatcher/backends/ssh_ray.py`
- Test: `tests/integration/test_backend_running_hosts.py` (new file)

**Interfaces:**
- Consumes: `LeaseService.current_hosts()` (Task 2) via the `HostLease` Ray actor (`self._actor.current_hosts.remote()`)
- Produces: `SshRayBackend.running_hosts(self) -> dict[str, str]`, satisfying `ExecutionBackend.running_hosts` (Task 3)

- [ ] **Step 1: Write the failing test**

```python
# tests/integration/test_backend_running_hosts.py
import time

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


def test_running_hosts_empty_before_setup(tmp_path):
    backend = SshRayBackend(results_dir=str(tmp_path))
    assert backend.running_hosts() == {}


def test_running_hosts_reports_host_while_job_in_flight(tmp_path, monkeypatch):
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=1),))
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: _canned_outcome("10.0.0.1"))

    # ponytail: inline closure so cloudpickle serializes it without a module reference
    def _slow_transport(host: RemoteHost) -> FakeTransport:
        def results(argv: list[str]) -> CommandResult:
            if 'printf %s "$HOME"' in " ".join(argv):
                return CommandResult(0, "/home/ubuntu", "", 0.0)
            if argv[0] == "python3":
                time.sleep(0.5)  # hold the lease long enough to observe it live
                return CommandResult(0, "", "", 0.0)
            if argv[0] == "cat":
                return CommandResult(0, '{"returncode": 0, "duration_s": 0.05}', "", 0.0)
            return CommandResult(0, "", "", 0.0)
        return FakeTransport(run_results=results)

    backend = SshRayBackend(transport_factory=_slow_transport, results_dir=str(tmp_path))
    try:
        backend.setup(inv, _project())
        handle = backend.submit("batch1", Job(id="j1", command=("echo", "hi")))
        time.sleep(0.2)  # let the Ray task acquire its lease and reach the slow step
        assert backend.running_hosts() == {"j1": "10.0.0.1"}
        result = backend.resolve(handle)
        assert result.status == JobStatus.SUCCEEDED
        assert backend.running_hosts() == {}  # lease released once the attempt finished
    finally:
        backend.teardown()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/micheleciavotta/Downloads/ray-dispatcher && uv run pytest tests/integration/test_backend_running_hosts.py -v`
Expected: FAIL — `test_running_hosts_empty_before_setup` fails with `TypeError: Can't instantiate abstract class SshRayBackend` (missing `running_hosts`).

- [ ] **Step 3: Implement**

In `src/ray_dispatcher/backends/ssh_ray.py`, add to `SshRayBackend` right after `resolve`:

```python
    def running_hosts(self) -> dict[str, str]:
        if self._actor is None:
            return {}
        return ray.get(self._actor.current_hosts.remote())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/micheleciavotta/Downloads/ray-dispatcher && uv run pytest tests/integration/test_backend_running_hosts.py -v`
Expected: PASS (2 tests). If `test_running_hosts_reports_host_while_job_in_flight` is flaky on slower machines, increase the `time.sleep(0.2)` margin — the job-side sleep is already a generous 0.5s.

- [ ] **Step 5: Commit**

```bash
cd /Users/micheleciavotta/Downloads/ray-dispatcher
git add src/ray_dispatcher/backends/ssh_ray.py tests/integration/test_backend_running_hosts.py
git commit -m "feat: implement SshRayBackend.running_hosts() via the HostLease actor"
```

---

### Task 5: `Dispatcher.running_hosts()`

**Files:**
- Modify: `src/ray_dispatcher/dispatcher.py`
- Modify: `tests/unit/test_dispatcher.py`

**Interfaces:**
- Consumes: `ExecutionBackend.running_hosts()` (Task 3/4)
- Produces: `Dispatcher.running_hosts(self) -> dict[str, str]` — the method `remote_experiments`' polling loop will call once per TUI tick.

- [ ] **Step 1: Write the failing test**

In `tests/unit/test_dispatcher.py`, add after `test_cancel_delegates_to_backend`:

```python
def test_running_hosts_delegates_to_backend():
    b = _mock_backend()
    b.running_hosts.return_value = {"j1": "10.0.0.1"}
    d = Dispatcher(_inv(), _proj(), backend=b)
    assert d.running_hosts() == {"j1": "10.0.0.1"}
    b.running_hosts.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/micheleciavotta/Downloads/ray-dispatcher && uv run pytest tests/unit/test_dispatcher.py::test_running_hosts_delegates_to_backend -v`
Expected: FAIL with `AttributeError: 'Dispatcher' object has no attribute 'running_hosts'`

- [ ] **Step 3: Implement**

In `src/ray_dispatcher/dispatcher.py`, add to `Dispatcher` right after `cancel`:

```python
    def running_hosts(self) -> dict[str, str]:
        return self._backend.running_hosts()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/micheleciavotta/Downloads/ray-dispatcher && uv run pytest tests/unit/test_dispatcher.py -v`
Expected: PASS (all tests in the file, including the new one)

- [ ] **Step 5: Commit**

```bash
cd /Users/micheleciavotta/Downloads/ray-dispatcher
git add src/ray_dispatcher/dispatcher.py tests/unit/test_dispatcher.py
git commit -m "feat: expose Dispatcher.running_hosts() for live VM progress views"
```

---

### Task 6: Export and full-suite verification

**Files:**
- Modify: `src/ray_dispatcher/__init__.py` (no new export needed — `running_hosts` is a method on the already-exported `Dispatcher`, not a new public name)
- No new files

**Interfaces:** None (verification-only task).

- [ ] **Step 1: Run the full unit + integration suite**

Run: `cd /Users/micheleciavotta/Downloads/ray-dispatcher && uv run pytest tests/unit tests/integration -v`
Expected: PASS, zero failures, zero errors (the `tests/e2e` directory stays excluded per `pyproject.toml`'s `addopts = "--ignore=tests/e2e"`).

- [ ] **Step 2: Run ruff and mypy**

Run: `cd /Users/micheleciavotta/Downloads/ray-dispatcher && uv run ruff check src tests && uv run mypy`
Expected: no errors. `mypy` is `strict = true` on `files = ["src"]` (per `pyproject.toml`) — the new methods all carry explicit `-> dict[str, str]` / `-> None` return types, so no new ignores should be needed.

- [ ] **Step 3: Update the design spec status**

In `docs/superpowers/specs/2026-06-30-live-host-visibility-design.md`, the doc has no `Status:` line update needed beyond what's already there ("In review") — leave as-is; this plan's completion is tracked by the checked-off tasks, not by editing the spec.

- [ ] **Step 4: Commit (only if Steps 1-2 required fixes)**

If ruff/mypy required any fixes, commit them now:

```bash
cd /Users/micheleciavotta/Downloads/ray-dispatcher
git add -A
git commit -m "fix: address ruff/mypy findings from running_hosts() addition"
```

If no fixes were needed, skip this commit — nothing to do.
