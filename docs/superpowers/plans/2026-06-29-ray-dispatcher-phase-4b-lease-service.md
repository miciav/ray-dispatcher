# ray-dispatcher — Phase 4b: Lease Service + Reconcile — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wrap the Phase 4a `LeasePool` in an async `LeaseService` (the body of the Ray `HostLease` actor) and add the SSH `reconcile_host` probe that terminates orphaned process groups, plus two small `LeasePool` additions the service needs.

**Architecture:** `LeaseService` holds a `LeasePool` and a single `asyncio.Condition`. `acquire()` waits on the condition while the pool is full and capacity remains, and raises `NoHealthyHostsError` the moment no healthy host is left — so no caller waits forever and no method blocks the event loop. `release`/`heartbeat`/`sweep`/`quarantine`/`mark_reconciled`/`quarantined_hosts` mutate the pool under the lock and notify waiters. All SSH work is kept OUT of the service: `reconcile_host(transport, pid_file)` is a standalone blocking function the caller runs off-actor (then calls `mark_reconciled`), reading the runner's `{"pid","pgid"}` file and terminating the recorded process group via Phase 2 `terminate_process_group`. `scheduling.py` stays Ray-free; the `ray.remote(num_cpus=0)` decoration of `LeaseService` is wired in Phase 6 where `ray.init` lives.

**Tech Stack:** Python ≥3.10 stdlib (`asyncio`, `json`), Phase 4a `LeasePool`/`Lease`, Phase 2 `Transport`/`terminate_process_group`, pytest/ruff/mypy. (`asyncio.run` drives the async tests — no pytest-asyncio.)

**Spec:** `docs/superpowers/specs/2026-06-27-ray-dispatcher-design.md` — §7.1 lease acquisition, §8.1 process termination, §8.2 lease safety + host loss, §3.2 exclusive runtime (`num_cpus=0`, `vm_slot`). Phases 1, 2, 3a, 3b, 4a are on `main`.

## Global Constraints

Every task implicitly includes these. Values copied verbatim from the spec.

- `requires-python = ">=3.10"`; mypy runs `strict` over `src` only; ruff `select = E,F,I,UP,B`, line-length 100.
- **`acquire()` waits on an `asyncio.Condition`, allowing release/heartbeat/reconciliation to run while jobs wait; no actor method blocks the event loop (§7.1).** A retry passes previously-attempted hosts as exclusions (forwarded to `LeasePool.acquire`).
- **No healthy capacity → `NoHealthyHostsError`; it does not wait forever (§8.2).** A waiter blocked on a full pool must wake and raise as soon as the last healthy host is lost.
- **Host loss (§8.2):** lease past heartbeat deadline → expired → host quarantined → a reconciliation probe reads the runner state and terminates any orphaned process group → host healthy only after reconciliation succeeds. A failed reconcile must leave the host visible for retry (`quarantined_hosts`).
- **Process termination (§8.1):** `SIGTERM` the recorded process group, bounded grace, then `SIGKILL`; complete only after a probe confirms the group is gone. Closing an SSH channel is never treated as termination. (`reconcile_host` delegates this to the existing `terminate_process_group`.)
- **No actor method does SSH:** SSH/reconciliation runs off the actor; `LeaseService` is pure in-memory async state.
- **Heartbeat-late policy (decided here):** `heartbeat` rejects (returns False) a lease whose deadline has passed (`now >= expiry_s`), even before a sweep removes it — a past-deadline lease is logically dead.

## Existing interfaces this plan composes (already on `main`)

- `scheduling.py` (Phase 4a): `Lease(token,host,slot,attempt_id,expiry_s,heartbeat_s)`; `LeasePool(hosts, *, lease_ttl_s=60.0, now=time.monotonic, token_factory=...)` with `acquire(attempt_id,*,exclude=())->Lease|None`, `release(token)->bool`, `heartbeat(token)->bool`, `quarantine(host)`, `mark_reconciled(host)`, `sweep_expired()->list[str]`, `healthy_host_count()->int`, `free_slots()->int`, and internals `_quarantined`, `_leases`, `_now`, `replace`.
- `ssh.py` (Phase 2): `Transport` Protocol (`run(argv,*,timeout_s=None)->CommandResult`); `CommandResult(returncode,stdout,stderr,duration_s)`; `FakeTransport(run_results=callback)` (callback gets `list(argv)`, `.calls` records `("run",tuple(argv))`); `terminate_process_group(transport, pgid, *, grace_s=10.0, poll_s=0.5, now=..., sleep=...) -> bool`.
- `remote_runner.py` (Phase 2): writes the pid file as JSON `{"pid": <int>, "pgid": <int>}`.
- `errors.py` (Phase 1): `NoHealthyHostsError(DispatcherError)`.

## Module note

Everything in this plan lands in `scheduling.py` (additions to `LeasePool`, the new `LeaseService`, and `reconcile_host`). `scheduling.py` gains `import asyncio` and `import json` and imports from `ssh`/`errors`, but does NOT import `ray` — the `ray.remote(num_cpus=0)(LeaseService)` decoration is Phase 6 (Dispatcher), so the pure pool stays importable and fast.

## Full-project file structure (only Phase 4b changes shown)

```text
src/ray_dispatcher/
├── … (Phases 1-3b)
├── scheduling.py     # THIS PLAN extends it: LeasePool.quarantined_hosts,
│                     # heartbeat late-rejection, reconcile_host, LeaseService
├── results.py        # Phase 5
├── backends/…        # Phase 5-6
└── dispatcher.py     # Phase 6 — decorates LeaseService as the HostLease actor
```

### Phase 4b file structure

- Modify: `src/ray_dispatcher/scheduling.py`
- Test: `tests/unit/test_lease_pool_quarantined_hosts.py`, `test_lease_pool_heartbeat_late.py`, `test_reconcile_host.py`, `test_lease_service_acquire.py`, `test_lease_service_methods.py`, `test_lease_service_no_capacity.py`

### Shared async test helper (copy inline into each LeaseService test file)

```python
import asyncio


class Clock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t
    def __call__(self) -> float:
        return self.t
    def advance(self, d: float) -> None:
        self.t += d


def _tokens():
    n = 0
    def factory() -> str:
        nonlocal n
        t = f"tok{n}"
        n += 1
        return t
    return factory
```

---

### Task 1: `LeasePool.quarantined_hosts()`

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (append method to `LeasePool`)
- Test: `tests/unit/test_lease_pool_quarantined_hosts.py`

**Interfaces:**
- Produces: `LeasePool.quarantined_hosts() -> list[str]` — the sorted list of currently quarantined hosts (so a caller can re-drive reconciliation for a host that a previous reconcile failed to clear).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_lease_pool_quarantined_hosts.py`:

```python
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


def test_quarantined_hosts_lists_quarantined_sorted():
    pool = LeasePool({"a": 1, "b": 1, "c": 1}, now=Clock(), token_factory=_tokens())
    assert pool.quarantined_hosts() == []
    pool.quarantine("c")
    pool.quarantine("a")
    assert pool.quarantined_hosts() == ["a", "c"]  # sorted


def test_quarantined_hosts_drops_after_reconcile():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    pool.quarantine("a")
    assert pool.quarantined_hosts() == ["a"]
    pool.mark_reconciled("a")
    assert pool.quarantined_hosts() == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_lease_pool_quarantined_hosts.py -v`
Expected: FAIL with `AttributeError: ... 'quarantined_hosts'`.

- [ ] **Step 3: Write the implementation (append to `LeasePool`)**

```python
    def quarantined_hosts(self) -> list[str]:
        return sorted(self._quarantined)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_lease_pool_quarantined_hosts.py -v`
Expected: both cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/scheduling.py tests/unit/test_lease_pool_quarantined_hosts.py
git commit -m "feat: add LeasePool.quarantined_hosts accessor"
```

---

### Task 2: `LeasePool.heartbeat` — reject a past-deadline lease

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (edit `heartbeat`)
- Test: `tests/unit/test_lease_pool_heartbeat_late.py`

**Interfaces:**
- Modifies: `LeasePool.heartbeat(token) -> bool` — now also returns False when `now >= lease.expiry_s` (the lease is past its deadline, even if a sweep has not yet removed it). Behaviour for live leases and unknown tokens is unchanged.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_lease_pool_heartbeat_late.py`:

```python
from ray_dispatcher.scheduling import LeasePool


class Clock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t
    def __call__(self) -> float:
        return self.t
    def advance(self, d: float) -> None:
        self.t += d


def _tokens():
    n = 0
    def factory() -> str:
        nonlocal n
        t = f"tok{n}"
        n += 1
        return t
    return factory


def test_heartbeat_rejects_past_deadline_even_before_sweep():
    clock = Clock(1000.0)
    pool = LeasePool({"a": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
    lease = pool.acquire("x")          # deadline 1060
    clock.advance(70.0)                # now 1070 >= 1060, NOT swept yet
    assert pool.heartbeat(lease.token) is False   # past deadline -> rejected
    # the lease was not extended (stored expiry is still the old deadline)
    assert pool._leases[lease.token].expiry_s == 1060.0


def test_heartbeat_still_extends_a_live_lease():
    clock = Clock(1000.0)
    pool = LeasePool({"a": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
    lease = pool.acquire("x")          # deadline 1060
    clock.advance(30.0)                # now 1030 < 1060 -> live
    assert pool.heartbeat(lease.token) is True
    assert pool._leases[lease.token].expiry_s == 1090.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_lease_pool_heartbeat_late.py -v`
Expected: FAIL on `test_heartbeat_rejects_past_deadline_even_before_sweep` (current `heartbeat` extends it and returns True).

- [ ] **Step 3: Edit the implementation**

In `LeasePool.heartbeat`, insert the deadline guard after reading `now`. The method becomes:

```python
    def heartbeat(self, token: str) -> bool:
        lease = self._leases.get(token)
        if lease is None:
            return False
        now = self._now()
        if now >= lease.expiry_s:
            return False  # past deadline (even if not yet swept) -> logically dead
        self._leases[token] = replace(lease, heartbeat_s=now, expiry_s=now + self._ttl)
        return True
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_lease_pool_heartbeat_late.py tests/unit/test_lease_pool_heartbeat.py tests/unit/test_lease_pool_sweep.py -v`
Expected: all pass — the new late-rejection cases AND the existing Phase 4a heartbeat/sweep tests (which never heartbeat after the deadline, so they are unaffected).

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/scheduling.py tests/unit/test_lease_pool_heartbeat_late.py
git commit -m "feat: reject heartbeat on a past-deadline lease"
```

---

### Task 3: `reconcile_host` — terminate an orphaned process group

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (append function + imports)
- Test: `tests/unit/test_reconcile_host.py`

**Interfaces:**
- Consumes: `Transport`, `terminate_process_group` (Phase 2); `json` (stdlib).
- Produces: `reconcile_host(transport: Transport, pid_file: str, *, grace_s: float = 10.0) -> bool` — reads the remote runner pid file (`{"pid","pgid"}`); returns True when the host is clean (no pid file → nothing orphaned, or the recorded process group is confirmed gone after termination); returns False when a pid file exists but is unparseable (cannot confirm clean → caller keeps the host quarantined).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_reconcile_host.py`:

```python
from ray_dispatcher.scheduling import reconcile_host
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _runs(t):
    return [c[1] for c in t.calls if c[0] == "run"]


def test_reconcile_no_pid_file_is_clean():
    def results(argv):
        if argv[0] == "cat":
            return CommandResult(1, "", "no such file", 0.0)
        return CommandResult(0, "", "", 0.0)

    t = FakeTransport(run_results=results)
    assert reconcile_host(t, "/home/u/.ray_dispatcher/runs/b/j/1/pid.json") is True
    assert not any(a[0] == "kill" for a in _runs(t))  # nothing terminated


def test_reconcile_terminates_recorded_pgid():
    def results(argv):
        if argv[0] == "cat":
            return CommandResult(0, '{"pid": 1234, "pgid": 4321}', "", 0.0)
        if argv[:2] == ["kill", "-0"]:
            return CommandResult(1, "", "", 0.0)  # probe: group already gone
        return CommandResult(0, "", "", 0.0)      # TERM/KILL succeed

    t = FakeTransport(run_results=results)
    assert reconcile_host(t, "/x/pid.json") is True
    assert ("kill", "-TERM", "-4321") in _runs(t)  # SIGTERM to the process group


def test_reconcile_corrupt_pid_file_stays_quarantined():
    def results(argv):
        if argv[0] == "cat":
            return CommandResult(0, "garbage{not json", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    t = FakeTransport(run_results=results)
    assert reconcile_host(t, "/x/pid.json") is False  # cannot confirm clean
    assert not any(a[0] == "kill" for a in _runs(t))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_reconcile_host.py -v`
Expected: FAIL with `ImportError: cannot import name 'reconcile_host'`.

- [ ] **Step 3: Write the implementation**

Add to the imports block of `scheduling.py`:

```python
import json
```

and MERGE `NoHealthyHostsError` into the existing errors import (do not add a second `from .errors import` line — the file already has `from .errors import ModelValidationError`):

```python
from .errors import ModelValidationError, NoHealthyHostsError
from .ssh import Transport, terminate_process_group
```

(`NoHealthyHostsError` is used by `LeaseService` in Task 6; adding it now keeps the import stable across tasks.)

Append to `scheduling.py` (module level, after `LeasePool`):

```python
def reconcile_host(transport: Transport, pid_file: str, *, grace_s: float = 10.0) -> bool:
    """Terminate any orphaned process group recorded for a lost attempt (spec §8.2/§8.1).

    Returns True when the host is confirmed clean: no pid file recorded, or the
    recorded process group is gone after SIGTERM/SIGKILL. Returns False when a pid
    file exists but cannot be parsed — the host stays quarantined for manual recovery.
    """
    result = transport.run(["cat", pid_file])
    if result.returncode != 0:
        return True  # no recorded process -> nothing orphaned
    try:
        pgid = int(json.loads(result.stdout)["pgid"])
    except (ValueError, KeyError, TypeError):
        return False  # recorded but unreadable -> cannot confirm clean
    return terminate_process_group(transport, pgid, grace_s=grace_s)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_reconcile_host.py -v`
Expected: all three cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/scheduling.py tests/unit/test_reconcile_host.py
git commit -m "feat: add reconcile_host (terminate orphaned process group)"
```

---

### Task 4: `LeaseService` — init + `acquire` (waits) + `release`

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (append class + `import asyncio`)
- Test: `tests/unit/test_lease_service_acquire.py`

**Interfaces:**
- Consumes: `LeasePool` (Phase 4a); `asyncio` (stdlib).
- Produces:
  - `LeaseService(hosts, *, lease_ttl_s=60.0, now=time.monotonic, token_factory=...)` — holds a `LeasePool` and an `asyncio.Condition`. (Constructor args mirror `LeasePool`; in production only `hosts`/`lease_ttl_s` are passed — `now`/`token_factory` exist for deterministic tests.)
  - `async LeaseService.acquire(attempt_id: str, exclude: Iterable[str] = ()) -> Lease` — under the condition, returns a lease as soon as one is available; while the pool is full (but capacity remains) it `await`s the condition. (The `NoHealthyHostsError` path is added in Task 6 — for this task the pool always has capacity in tests.)
  - `async LeaseService.release(token: str) -> None` — releases the slot and notifies waiters.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_lease_service_acquire.py`:

```python
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


def test_acquire_returns_lease_immediately_when_free():
    async def scenario():
        svc = LeaseService({"a": 1}, now=Clock(), token_factory=_tokens())
        lease = await svc.acquire("job/1")
        assert lease is not None and lease.host == "a"

    asyncio.run(scenario())


def test_acquire_blocks_until_release():
    async def scenario():
        svc = LeaseService({"a": 1}, now=Clock(), token_factory=_tokens())
        first = await svc.acquire("job/1")     # consumes the only slot
        waiter = asyncio.create_task(svc.acquire("job/2"))
        await asyncio.sleep(0.05)              # let the waiter block on the condition
        assert not waiter.done()               # no capacity -> still waiting
        await svc.release(first.token)         # frees the slot, notifies
        second = await asyncio.wait_for(waiter, timeout=1.0)
        assert second is not None and second.host == "a"

    asyncio.run(scenario())
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_lease_service_acquire.py -v`
Expected: FAIL with `ImportError: cannot import name 'LeaseService'`.

- [ ] **Step 3: Write the implementation**

Add to the imports block of `scheduling.py`:

```python
import asyncio
```

Append to `scheduling.py` (module level, after `reconcile_host`):

```python
class LeaseService:
    """Async wrapper over LeasePool, the body of the Ray HostLease actor (§7.1).

    Pure in-memory async state guarded by one asyncio.Condition — no SSH, so no
    method blocks the event loop. Phase 6 decorates this class with
    ``ray.remote(num_cpus=0)``; reconciliation (``reconcile_host``) runs off the
    actor and reports back via ``mark_reconciled``.
    """

    def __init__(
        self,
        hosts: Mapping[str, int],
        *,
        lease_ttl_s: float = 60.0,
        now: Callable[[], float] = time.monotonic,
        token_factory: Callable[[], str] = lambda: secrets.token_hex(16),
    ) -> None:
        self._pool = LeasePool(
            hosts, lease_ttl_s=lease_ttl_s, now=now, token_factory=token_factory
        )
        self._cond = asyncio.Condition()

    async def acquire(self, attempt_id: str, exclude: Iterable[str] = ()) -> Lease:
        async with self._cond:
            while True:
                lease = self._pool.acquire(attempt_id, exclude=exclude)
                if lease is not None:
                    return lease
                await self._cond.wait()

    async def release(self, token: str) -> None:
        async with self._cond:
            self._pool.release(token)
            self._cond.notify_all()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_lease_service_acquire.py -v`
Expected: both cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/scheduling.py tests/unit/test_lease_service_acquire.py
git commit -m "feat: add LeaseService with condition-waiting acquire + release"
```

---

### Task 5: `LeaseService` — heartbeat / sweep / quarantine / mark_reconciled / quarantined_hosts

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (append methods to `LeaseService`)
- Test: `tests/unit/test_lease_service_methods.py`

**Interfaces:**
- Consumes: `LeaseService` (Task 4); `LeasePool` methods (Phase 4a + Task 1).
- Produces (all `async`, all mutate under the condition and notify waiters so a capacity change re-wakes them):
  - `heartbeat(token) -> bool` (delegates to pool; no notify needed — extending a lease frees nothing).
  - `sweep() -> list[str]` (pool.sweep_expired; notify so waiters re-check capacity; returns hosts to reconcile).
  - `quarantine(host) -> None` (notify — capacity may have dropped).
  - `mark_reconciled(host) -> None` (notify — capacity restored).
  - `quarantined_hosts() -> list[str]`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_lease_service_methods.py`:

```python
import asyncio

from ray_dispatcher.scheduling import LeaseService


class Clock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t
    def __call__(self) -> float:
        return self.t
    def advance(self, d: float) -> None:
        self.t += d


def _tokens():
    n = 0
    def factory() -> str:
        nonlocal n
        t = f"tok{n}"
        n += 1
        return t
    return factory


def test_heartbeat_delegates():
    async def scenario():
        svc = LeaseService({"a": 1}, lease_ttl_s=60.0, now=Clock(), token_factory=_tokens())
        lease = await svc.acquire("x")
        assert await svc.heartbeat(lease.token) is True
        assert await svc.heartbeat("nope") is False

    asyncio.run(scenario())


def test_sweep_returns_expired_hosts_and_wakes_waiters():
    async def scenario():
        clock = Clock(1000.0)
        svc = LeaseService({"a": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
        await svc.acquire("x")           # deadline 1060
        clock.advance(100.0)             # past deadline
        hosts = await svc.sweep()
        assert hosts == ["a"]            # quarantined, reported for reconcile
        assert await svc.quarantined_hosts() == ["a"]

    asyncio.run(scenario())


def test_mark_reconciled_restores_and_lets_acquire_proceed():
    async def scenario():
        clock = Clock(1000.0)
        svc = LeaseService({"a": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
        await svc.acquire("x")
        clock.advance(100.0)
        await svc.sweep()                # 'a' quarantined, lease dropped
        assert await svc.quarantined_hosts() == ["a"]
        await svc.mark_reconciled("a")
        assert await svc.quarantined_hosts() == []
        lease = await asyncio.wait_for(svc.acquire("y"), timeout=1.0)  # usable again
        assert lease.host == "a"

    asyncio.run(scenario())
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_lease_service_methods.py -v`
Expected: FAIL with `AttributeError: 'LeaseService' object has no attribute 'heartbeat'`.

- [ ] **Step 3: Write the implementation (append to `LeaseService`)**

```python
    async def heartbeat(self, token: str) -> bool:
        async with self._cond:
            return self._pool.heartbeat(token)

    async def sweep(self) -> list[str]:
        async with self._cond:
            hosts = self._pool.sweep_expired()
            if hosts:
                self._cond.notify_all()  # capacity may have dropped -> re-check waiters
            return hosts

    async def quarantine(self, host: str) -> None:
        async with self._cond:
            self._pool.quarantine(host)
            self._cond.notify_all()

    async def mark_reconciled(self, host: str) -> None:
        async with self._cond:
            self._pool.mark_reconciled(host)
            self._cond.notify_all()

    async def quarantined_hosts(self) -> list[str]:
        async with self._cond:
            return self._pool.quarantined_hosts()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_lease_service_methods.py -v`
Expected: all three cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/scheduling.py tests/unit/test_lease_service_methods.py
git commit -m "feat: add LeaseService heartbeat/sweep/quarantine/mark_reconciled/quarantined_hosts"
```

---

### Task 6: `LeaseService.acquire` — raise `NoHealthyHostsError` when capacity is gone

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (edit `acquire`)
- Test: `tests/unit/test_lease_service_no_capacity.py`

**Interfaces:**
- Modifies: `LeaseService.acquire` — inside the wait loop, after a `None` from the pool, raise `NoHealthyHostsError` when `self._pool.healthy_host_count() == 0`; otherwise `await` the condition. A waiter already blocked must wake (via the notify in `quarantine`/`sweep`) and raise when the last host is lost.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_lease_service_no_capacity.py`:

```python
import asyncio

import pytest

from ray_dispatcher.errors import NoHealthyHostsError
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


def test_acquire_raises_immediately_when_no_capacity():
    async def scenario():
        svc = LeaseService({"a": 1}, now=Clock(), token_factory=_tokens())
        await svc.acquire("x")        # consume the slot
        await svc.quarantine("a")     # now zero healthy hosts
        # wait_for so the pre-fix version (which would block on the condition)
        # fails cleanly with TimeoutError instead of hanging the whole run.
        with pytest.raises(NoHealthyHostsError):
            await asyncio.wait_for(svc.acquire("y"), timeout=1.0)

    asyncio.run(scenario())


def test_blocked_acquire_wakes_and_raises_when_last_host_lost():
    async def scenario():
        svc = LeaseService({"a": 1}, now=Clock(), token_factory=_tokens())
        await svc.acquire("x")                       # full
        waiter = asyncio.create_task(svc.acquire("y"))
        await asyncio.sleep(0.05)                     # waiter blocks (capacity remains)
        assert not waiter.done()
        await svc.quarantine("a")                     # last host lost -> notify
        with pytest.raises(NoHealthyHostsError):
            await asyncio.wait_for(waiter, timeout=1.0)

    asyncio.run(scenario())
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_lease_service_no_capacity.py -v`
Expected: both cases FAIL (cleanly, in ~1s each). Pre-fix, `acquire` blocks on the condition instead of raising, so the `asyncio.wait_for` wrapper raises `TimeoutError` — which `pytest.raises(NoHealthyHostsError)` does not catch — and each test fails. No hang.

- [ ] **Step 3: Edit the implementation**

Update `LeaseService.acquire` to raise when capacity is gone:

```python
    async def acquire(self, attempt_id: str, exclude: Iterable[str] = ()) -> Lease:
        async with self._cond:
            while True:
                lease = self._pool.acquire(attempt_id, exclude=exclude)
                if lease is not None:
                    return lease
                if self._pool.healthy_host_count() == 0:
                    raise NoHealthyHostsError("no healthy hosts remain")
                await self._cond.wait()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_lease_service_no_capacity.py tests/unit/test_lease_service_acquire.py -v`
Expected: all pass — the no-capacity cases raise, and the Task 4 wait-then-release case still works.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/scheduling.py tests/unit/test_lease_service_no_capacity.py
git commit -m "feat: LeaseService.acquire raises NoHealthyHostsError when capacity is gone"
```

---

### Task 7: Phase 4b gate

**Files:**
- (no new source) — final full-toolchain verification.

**Interfaces:** none.

- [ ] **Step 1: Run the full suite**

Run: `uv run pytest -q`
Expected: all tests `passed` (Phases 1-4a + Phase 4b). The async `LeaseService` tests run under `asyncio.run`; no event loop leaks between tests.

- [ ] **Step 2: Lint + type check**

Run: `uv run ruff check --fix . && uv run ruff check . && uv run mypy`
Expected: ruff auto-sorts then reports `All checks passed!`; mypy reports `Success: no issues found`.

If mypy flags `asyncio.Condition` / async return types, fix minimally (no config relaxation). Confirm `scheduling.py` does NOT import `ray` (the actor decoration is Phase 6).

- [ ] **Step 3: Commit (only if Step 2 made changes)**

```bash
git add -A
git commit -m "chore: phase 4b gate green (ruff + mypy)"
```

---

## Phase 4b self-review

Run before declaring Phase 4b done:

- [ ] `LeasePool.quarantined_hosts()` returns the sorted quarantined set so a failed reconcile leaves the host visible for retry (§8.2).
- [ ] `LeasePool.heartbeat` rejects a past-deadline lease (`now >= expiry_s`) even before sweep; live leases still extend; unknown tokens still False.
- [ ] `reconcile_host` reads `{"pid","pgid"}`, terminates the recorded group via `terminate_process_group` (SIGTERM→grace→SIGKILL→probe, §8.1), returns True when clean (no file / group gone) and False on an unparseable file (stays quarantined).
- [ ] `LeaseService.acquire` waits on the `asyncio.Condition` while full-with-capacity and raises `NoHealthyHostsError` the moment `healthy_host_count()==0`; a blocked waiter wakes and raises when the last host is lost (§7.1, §8.2).
- [ ] `release`/`sweep`/`quarantine`/`mark_reconciled` notify waiters; no `LeaseService` method does SSH (reconciliation runs off the actor).
- [ ] `scheduling.py` imports `asyncio`/`json`/`ssh`/`errors` but NOT `ray`.
- [ ] `uv run pytest -q`, `uv run ruff check .`, and `uv run mypy` all green.

**Deliverable:** `scheduling.py` extended with `quarantined_hosts`, heartbeat late-rejection, `reconcile_host`, and the async `LeaseService` — the full body of the Ray `HostLease` actor, tested with `asyncio.run` and `FakeTransport`.

**Residuals carried to later phases (documented):**
- **Phase 6 (Dispatcher):** decorate `HostLease = ray.remote(num_cpus=0)(LeaseService)`; declare the `vm_slot` resource; drive the loop — `acquire` per attempt, `heartbeat` while the remote process runs, on lease loss run `reconcile_host` off-actor then `mark_reconciled` (re-driving from `quarantined_hosts` if a reconcile failed); release after the process is confirmed terminated.
- **Phase 6 (Dispatcher):** §3.2.6 stale-lock reconciliation — before a `SessionLock` stale-takeover, terminate orphaned process groups from the prior session's run dirs (using `reconcile_host`) and only take over when clean. Needs the Phase 5 run-dir layout + the Dispatcher's session knowledge, so it lands with the Dispatcher, not in `scheduling.py`.
- **Phase 5:** the attempt task supplies the concrete `pid_file` path (`runs/<batch>/<job>/<attempt>/pid.json`) that `reconcile_host` consumes.
