# ray-dispatcher — Phase 4a: Lease Pool — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the pure scheduling state machine — `Lease` + `LeasePool` — that mints per-host slot leases, expires and quarantines lost ones, and reinstates hosts after reconciliation.

**Architecture:** `LeasePool` is a plain, single-threaded, clock-injected state machine with no Ray and no SSH. It owns the healthy/quarantined host sets, per-host free-slot counts, and active leases keyed by random token. `acquire()` honours retry exclusions (reuse a host only after every healthy host has been tried) and returns `None` when nothing is free; `release()`/`heartbeat()` are token-checked and idempotent; `sweep_expired()` quarantines hosts whose leases passed their heartbeat deadline; `mark_reconciled()` returns a host to service. Phase 4b wraps this in the async `HostLease` Ray actor and adds the SSH reconciliation probe.

**Tech Stack:** Python ≥3.10 stdlib (`secrets`, `time`, `dataclasses`), pytest/ruff/mypy.

**Spec:** `docs/superpowers/specs/2026-06-27-ray-dispatcher-design.md` — §7.1 lease acquisition, §8.2 lease safety, §8.3 retry policy, §3.2 exclusive runtime. Phases 1, 2, 3a, 3b are on `main`.

## Global Constraints

Every task implicitly includes these. Values copied verbatim from the spec.

- `requires-python = ">=3.10"`; mypy runs `strict` over `src` only; ruff `select = E,F,I,UP,B`, line-length 100.
- **A lease contains a random token, host, slot, attempt identity, expiry, and heartbeat timestamp (§8.2).**
- **Acquire honours exclusions (§7.1):** a retry passes previously-attempted hosts as exclusions; a host is reused only after *every* healthy host has been tried. While any non-excluded healthy host still exists (even if momentarily full), an excluded host is not reused — the caller waits.
- **Release is token-checked and idempotent (§8.2).** Releasing an unknown/already-released token is a no-op, not an error.
- **Heartbeat is token-checked (§8.2):** heartbeating an unknown or already-expired token fails (returns False) and does not resurrect a lease.
- **On lease loss (§8.2):** a lease past its heartbeat deadline is expired; its host is *quarantined* (not returned to the pool); the host becomes healthy again only after reconciliation succeeds (`mark_reconciled`). Quarantining a host invalidates every lease on it.
- **No healthy capacity (§8.2):** the pool exposes the healthy (non-quarantined) host count so the caller (Phase 4b actor) can fail pending work with `NoHealthyHostsError` instead of waiting forever. (Raising that error is Phase 4b's job; 4a only reports capacity.)
- The pool is single-threaded and deterministic: all time comes from an injected `now: Callable[[], float]` (monotonic by default), all tokens from an injected `token_factory: Callable[[], str]` (`secrets.token_hex(16)` by default).

## Module note

Spec §5 lists `scheduling.py` for this area. Phase 4 splits it: 4a builds the pure `Lease`/`LeasePool` here; 4b adds the `HostLease` Ray actor, the `reconcile_host` SSH probe, and the stale-lock reconciliation — all composing this pool. Internal split for testability; no architecture change.

## Existing interfaces (already on `main`)

- `errors.py`: `ModelValidationError(ConfigurationError)`, `NoHealthyHostsError(DispatcherError)`.
- `models.py`: `RetryPolicy(max_attempts=2, retry_on={SSH,HOST_LOST,COLLECTION})`, `FailureKind`, `JobStatus`, `AttemptResult`, `JobResult`, `JobHandle(batch_id, job_id, token)`. (Phase 4a does not modify models; 4b/5 consume the retry/result types.)

## Full-project file structure (only Phase 4a file is built here)

```text
src/ray_dispatcher/
├── errors.py / paths.py / models.py        # Phase 1
├── ssh.py / remote_runner.py               # Phase 2
├── digests.py / locking.py                 # Phase 3a
├── provisioning.py                         # Phase 3b
├── scheduling.py     # THIS PLAN — Lease, LeasePool (pure state machine)
│                     # Phase 4b appends HostLease actor + reconcile_host
├── results.py        # Phase 5
├── backends/…        # Phase 5-6
└── dispatcher.py     # Phase 6
```

### Phase 4a file structure

- Create: `src/ray_dispatcher/scheduling.py`
- Test: `tests/unit/test_lease.py`, `test_lease_pool_acquire.py`, `test_lease_pool_exclusions.py`, `test_lease_pool_release.py`, `test_lease_pool_heartbeat.py`, `test_lease_pool_quarantine.py`, `test_lease_pool_sweep.py`

### Shared test helper (copy inline into each test file that needs it)

```python
class Clock:
    """Deterministic injectable clock."""
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t
    def __call__(self) -> float:
        return self.t
    def advance(self, d: float) -> None:
        self.t += d


def _tokens():
    """Deterministic token factory: tok0, tok1, ..."""
    n = 0
    def factory() -> str:
        nonlocal n
        t = f"tok{n}"
        n += 1
        return t
    return factory
```

---

### Task 1: `Lease` dataclass

**Files:**
- Create: `src/ray_dispatcher/scheduling.py`
- Test: `tests/unit/test_lease.py`

**Interfaces:**
- Consumes: `ModelValidationError` (Phase 1).
- Produces: frozen `Lease(token: str, host: str, slot: int, attempt_id: str, expiry_s: float, heartbeat_s: float)` validating non-empty token/host/attempt_id and `slot >= 0`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_lease.py`:

```python
import pytest

from ray_dispatcher.errors import ModelValidationError
from ray_dispatcher.scheduling import Lease


def test_lease_holds_all_fields():
    lease = Lease(token="t", host="ubuntu@a:22", slot=0, attempt_id="b/j/1",
                  expiry_s=1060.0, heartbeat_s=1000.0)
    assert lease.token == "t"
    assert lease.host == "ubuntu@a:22"
    assert lease.slot == 0
    assert lease.attempt_id == "b/j/1"
    assert lease.expiry_s == 1060.0 and lease.heartbeat_s == 1000.0


def test_lease_is_frozen():
    lease = Lease(token="t", host="a", slot=0, attempt_id="x", expiry_s=1.0, heartbeat_s=0.0)
    with pytest.raises(Exception):
        lease.token = "other"  # type: ignore[misc]


@pytest.mark.parametrize("kw", [
    {"token": ""},
    {"host": ""},
    {"attempt_id": ""},
    {"slot": -1},
])
def test_lease_rejects_invalid(kw):
    base = dict(token="t", host="a", slot=0, attempt_id="x", expiry_s=1.0, heartbeat_s=0.0)
    base.update(kw)
    with pytest.raises(ModelValidationError):
        Lease(**base)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_lease.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ray_dispatcher.scheduling'`.

- [ ] **Step 3: Write the implementation**

`src/ray_dispatcher/scheduling.py`:

```python
"""Scheduling state machine: per-host slot leases with quarantine (spec §7.1, §8.2).

`LeasePool` is a pure, single-threaded, clock-injected state machine — no Ray,
no SSH. Phase 4b wraps it in the async HostLease Ray actor and adds the SSH
reconciliation probe.
"""

from __future__ import annotations

import secrets
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace

from .errors import ModelValidationError


@dataclass(frozen=True)
class Lease:
    token: str
    host: str
    slot: int
    attempt_id: str
    expiry_s: float
    heartbeat_s: float

    def __post_init__(self) -> None:
        if not self.token:
            raise ModelValidationError("lease token must be non-empty")
        if not self.host:
            raise ModelValidationError("lease host must be non-empty")
        if not self.attempt_id:
            raise ModelValidationError("lease attempt_id must be non-empty")
        if self.slot < 0:
            raise ModelValidationError(f"lease slot must be >= 0, got {self.slot}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_lease.py -v`
Expected: all cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/scheduling.py tests/unit/test_lease.py
git commit -m "feat: add Lease dataclass"
```

---

### Task 2: `LeasePool.__init__` + capacity introspection

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (append)
- Test: `tests/unit/test_lease_pool_acquire.py` (init portion)

**Interfaces:**
- Consumes: `Lease` (Task 1).
- Produces:
  - `LeasePool(hosts: Mapping[str, int], *, lease_ttl_s: float = 60.0, now: Callable[[], float] = time.monotonic, token_factory: Callable[[], str] = ...)` — `hosts` maps a host label to its slot count.
  - `LeasePool.healthy_host_count() -> int` (healthy minus quarantined).
  - `LeasePool.free_slots() -> int` (free slots across healthy, non-quarantined hosts).
  - internal `_free_slot_count(host) -> int`, `_take_slot(host) -> int`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_lease_pool_acquire.py` (start the file; more tests appended in Tasks 3-4):

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


def _pool(**kw):
    return LeasePool({"a": 2, "b": 1}, now=Clock(), token_factory=_tokens(), **kw)


def test_initial_capacity():
    pool = _pool()
    assert pool.healthy_host_count() == 2
    assert pool.free_slots() == 3  # 2 + 1


def test_empty_inventory_has_no_capacity():
    pool = LeasePool({}, now=Clock(), token_factory=_tokens())
    assert pool.healthy_host_count() == 0
    assert pool.free_slots() == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_lease_pool_acquire.py -v`
Expected: FAIL with `ImportError: cannot import name 'LeasePool'`.

- [ ] **Step 3: Write the implementation (append to `scheduling.py`)**

```python
class LeasePool:
    """Per-host slot leases with quarantine. Single-threaded; deterministic via
    injected `now` and `token_factory` (spec §7.1, §8.2)."""

    def __init__(
        self,
        hosts: Mapping[str, int],
        *,
        lease_ttl_s: float = 60.0,
        now: Callable[[], float] = time.monotonic,
        token_factory: Callable[[], str] = lambda: secrets.token_hex(16),
    ) -> None:
        self._slots: dict[str, int] = dict(hosts)
        self._healthy: set[str] = set(hosts)
        self._quarantined: set[str] = set()
        self._used: dict[str, set[int]] = {h: set() for h in hosts}
        self._leases: dict[str, Lease] = {}
        self._ttl = lease_ttl_s
        self._now = now
        self._token_factory = token_factory

    def _live_hosts(self) -> set[str]:
        return self._healthy - self._quarantined

    def _free_slot_count(self, host: str) -> int:
        return self._slots[host] - len(self._used[host])

    def _take_slot(self, host: str) -> int:
        used = self._used[host]
        slot = next(i for i in range(self._slots[host]) if i not in used)
        used.add(slot)
        return slot

    def healthy_host_count(self) -> int:
        return len(self._live_hosts())

    def free_slots(self) -> int:
        return sum(self._free_slot_count(h) for h in self._live_hosts())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_lease_pool_acquire.py -v`
Expected: both cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/scheduling.py tests/unit/test_lease_pool_acquire.py
git commit -m "feat: add LeasePool init + capacity introspection"
```

---

### Task 3: `LeasePool.acquire` — happy path

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (append method)
- Test: `tests/unit/test_lease_pool_acquire.py` (append)

**Interfaces:**
- Consumes: Task 2 internals; `Lease` (Task 1).
- Produces: `LeasePool.acquire(attempt_id: str, *, exclude: Iterable[str] = ()) -> Lease | None` — mints a lease on a live host with a free slot (most-free-first), with `expiry_s = now + ttl`; returns `None` when no slot is available. (Exclusion/reuse semantics land in Task 4; this task covers the no-exclusion path.)

- [ ] **Step 1: Write the failing test (append to `test_lease_pool_acquire.py`)**

```python
def test_acquire_mints_lease_and_consumes_slot():
    clock = Clock(1000.0)
    pool = LeasePool({"a": 2, "b": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
    lease = pool.acquire("batch/job/1")
    assert lease is not None
    assert lease.token == "tok0"
    assert lease.host in ("a", "b")
    assert lease.attempt_id == "batch/job/1"
    assert lease.heartbeat_s == 1000.0 and lease.expiry_s == 1060.0
    assert pool.free_slots() == 2  # one consumed


def test_acquire_prefers_most_free_host():
    pool = LeasePool({"a": 3, "b": 1}, now=Clock(), token_factory=_tokens())
    lease = pool.acquire("x")
    assert lease.host == "a"  # 3 free > 1 free


def test_acquire_returns_none_when_full():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    assert pool.acquire("x") is not None
    assert pool.acquire("y") is None  # only slot taken


def test_distinct_slots_on_same_host():
    pool = LeasePool({"a": 2}, now=Clock(), token_factory=_tokens())
    l1 = pool.acquire("x")
    l2 = pool.acquire("y")
    assert {l1.slot, l2.slot} == {0, 1}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_lease_pool_acquire.py -v`
Expected: FAIL with `AttributeError: 'LeasePool' object has no attribute 'acquire'`.

- [ ] **Step 3: Write the implementation (append to `LeasePool`)**

```python
    def acquire(self, attempt_id: str, *, exclude: Iterable[str] = ()) -> Lease | None:
        live = self._live_hosts()
        untried = live - set(exclude)
        # Reuse an excluded host only once every healthy host has been tried (§7.1):
        # while any non-excluded host exists, draw only from those (else wait).
        pool = untried if untried else live
        candidates = [h for h in pool if self._free_slot_count(h) > 0]
        if not candidates:
            return None
        # most-free first; sort the labels so ties resolve deterministically.
        host = max(sorted(candidates), key=self._free_slot_count)
        slot = self._take_slot(host)
        heartbeat = self._now()
        lease = Lease(
            token=self._token_factory(),
            host=host,
            slot=slot,
            attempt_id=attempt_id,
            expiry_s=heartbeat + self._ttl,
            heartbeat_s=heartbeat,
        )
        self._leases[lease.token] = lease
        return lease
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_lease_pool_acquire.py -v`
Expected: all cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/scheduling.py tests/unit/test_lease_pool_acquire.py
git commit -m "feat: add LeasePool.acquire (mint lease, consume slot)"
```

---

### Task 4: `LeasePool.acquire` — exclusions and reuse-after-all-tried

**Files:**
- Test: `tests/unit/test_lease_pool_exclusions.py`

**Interfaces:**
- Consumes: `LeasePool.acquire` (Task 3) — this task adds tests that pin the exclusion/reuse branch already implemented in Task 3. If a branch is wrong, fix `acquire` minimally and note it.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_lease_pool_exclusions.py`:

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


def test_excluded_host_is_skipped_when_another_is_free():
    pool = LeasePool({"a": 1, "b": 1}, now=Clock(), token_factory=_tokens())
    lease = pool.acquire("retry", exclude=["a"])
    assert lease.host == "b"  # 'a' excluded, 'b' chosen


def test_waits_rather_than_reuse_while_untried_host_exists():
    # Fill 'b' (the only non-excluded host), leave 'a' free but excluded.
    # 'b' is still untried-but-full, so the retry must wait (None), not reuse 'a'.
    pool = LeasePool({"a": 1, "b": 1}, now=Clock(), token_factory=_tokens())
    b_lease = pool.acquire("on_b", exclude=["a"])   # picks 'b' ('a' excluded)
    assert b_lease.host == "b"
    # now 'b' is full, 'a' is free but excluded and still untried -> wait (None)
    assert pool.acquire("retry", exclude=["a"]) is None


def test_reuses_excluded_host_once_all_healthy_tried():
    # single host, already tried -> retry may reuse it (it is the only healthy host).
    pool = LeasePool({"a": 2}, now=Clock(), token_factory=_tokens())
    first = pool.acquire("first")
    assert first.host == "a"
    reuse = pool.acquire("retry", exclude=["a"])  # every healthy host tried -> reuse 'a'
    assert reuse is not None and reuse.host == "a"
    assert reuse.slot != first.slot
```

- [ ] **Step 2: Run test to verify it passes (branch implemented in Task 3)**

Run: `uv run pytest tests/unit/test_lease_pool_exclusions.py -v`
Expected: all three cases `passed`. If any fails, the exclusion/reuse branch in `acquire` is wrong — fix it minimally and re-run.

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_lease_pool_exclusions.py
git commit -m "test: pin LeasePool exclusion + reuse-after-all-tried"
```

---

### Task 5: `LeasePool.release` — token-checked, idempotent

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (append method)
- Test: `tests/unit/test_lease_pool_release.py`

**Interfaces:**
- Consumes: Task 2/3 internals.
- Produces: `LeasePool.release(token: str) -> bool` — frees the lease's slot and returns True; returns False (no-op) for an unknown/already-released token.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_lease_pool_release.py`:

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


def test_release_frees_slot():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    lease = pool.acquire("x")
    assert pool.free_slots() == 0
    assert pool.release(lease.token) is True
    assert pool.free_slots() == 1
    # slot is reusable
    assert pool.acquire("y") is not None


def test_release_is_idempotent():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    lease = pool.acquire("x")
    assert pool.release(lease.token) is True
    assert pool.release(lease.token) is False  # already released -> no-op
    assert pool.free_slots() == 1  # not double-freed


def test_release_unknown_token_is_noop():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    assert pool.release("nope") is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_lease_pool_release.py -v`
Expected: FAIL with `AttributeError: ... 'release'`.

- [ ] **Step 3: Write the implementation (append to `LeasePool`)**

```python
    def release(self, token: str) -> bool:
        lease = self._leases.pop(token, None)
        if lease is None:
            return False
        self._used[lease.host].discard(lease.slot)
        return True
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_lease_pool_release.py -v`
Expected: all cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/scheduling.py tests/unit/test_lease_pool_release.py
git commit -m "feat: add LeasePool.release (token-checked, idempotent)"
```

---

### Task 6: `LeasePool.heartbeat` — token-checked, extends expiry

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (append method)
- Test: `tests/unit/test_lease_pool_heartbeat.py`

**Interfaces:**
- Consumes: Task 2/3 internals; `dataclasses.replace` (imported in Task 1).
- Produces: `LeasePool.heartbeat(token: str) -> bool` — updates the lease's `heartbeat_s`/`expiry_s` to `now`/`now + ttl` and returns True; returns False for an unknown/expired token (does not resurrect a lease).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_lease_pool_heartbeat.py`:

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


def test_heartbeat_extends_expiry():
    clock = Clock(1000.0)
    pool = LeasePool({"a": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
    lease = pool.acquire("x")
    assert lease.expiry_s == 1060.0
    clock.advance(30.0)  # now 1030
    assert pool.heartbeat(lease.token) is True
    # the stored lease now carries a fresh deadline (now + ttl)
    assert pool._leases[lease.token].heartbeat_s == 1030.0
    assert pool._leases[lease.token].expiry_s == 1090.0


def test_heartbeat_unknown_token_false():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    assert pool.heartbeat("nope") is False
```

(The "heartbeat keeps a lease alive past its old deadline" and "heartbeat after
expiry does not resurrect" behaviours are pinned in Task 7, where `sweep_expired`
exists.)

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_lease_pool_heartbeat.py -v`
Expected: FAIL with `AttributeError: ... 'heartbeat'`.

- [ ] **Step 3: Write the implementation (append to `LeasePool`)**

```python
    def heartbeat(self, token: str) -> bool:
        lease = self._leases.get(token)
        if lease is None:
            return False
        now = self._now()
        self._leases[token] = replace(lease, heartbeat_s=now, expiry_s=now + self._ttl)
        return True
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_lease_pool_heartbeat.py -v`
Expected: both cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/scheduling.py tests/unit/test_lease_pool_heartbeat.py
git commit -m "feat: add LeasePool.heartbeat (token-checked, extends expiry)"
```

---

### Task 7: `quarantine` + `mark_reconciled` + `sweep_expired`

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (append three methods)
- Test: `tests/unit/test_lease_pool_quarantine.py`, `tests/unit/test_lease_pool_sweep.py`

**Interfaces:**
- Consumes: Task 2/3/5 internals.
- Produces:
  - `LeasePool.quarantine(host: str) -> None` — marks the host quarantined and drops every lease on it (freeing their slot bookkeeping); a quarantined host is excluded from `acquire`/capacity.
  - `LeasePool.mark_reconciled(host: str) -> None` — returns a quarantined host to the healthy pool.
  - `LeasePool.sweep_expired() -> list[str]` — quarantines every host that has a lease past its `expiry_s`, and returns the sorted distinct list of newly-affected host labels (for the caller to reconcile).

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_lease_pool_quarantine.py`:

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


def test_quarantine_removes_host_from_capacity_and_drops_its_leases():
    pool = LeasePool({"a": 2, "b": 1}, now=Clock(), token_factory=_tokens())
    la = pool.acquire("x")  # lands on 'a' (most free)
    assert la.host == "a"
    pool.quarantine("a")
    assert pool.healthy_host_count() == 1  # only 'b'
    assert pool.acquire("y").host == "b"  # acquire avoids quarantined 'a'
    # 'a' lease is invalid; releasing it is a no-op
    assert pool.release(la.token) is False


def test_acquire_skips_quarantined_host():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    pool.quarantine("a")
    assert pool.acquire("x") is None
    assert pool.free_slots() == 0


def test_mark_reconciled_restores_host():
    pool = LeasePool({"a": 1}, now=Clock(), token_factory=_tokens())
    pool.quarantine("a")
    assert pool.acquire("x") is None
    pool.mark_reconciled("a")
    assert pool.healthy_host_count() == 1
    assert pool.acquire("x") is not None  # slots usable again after reconcile
```

`tests/unit/test_lease_pool_sweep.py`:

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


def test_sweep_quarantines_expired_lease_host():
    clock = Clock(1000.0)
    pool = LeasePool({"a": 1, "b": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
    pool.acquire("x")           # on 'a' or 'b'; both have 1 slot -> 'a' (sorted/most-free tie)
    clock.advance(100.0)        # past expiry 1060
    affected = pool.sweep_expired()
    assert len(affected) == 1
    assert affected[0] in ("a", "b")
    assert affected[0] not in pool._live_hosts()  # quarantined
    assert pool.healthy_host_count() == 1


def test_sweep_keeps_live_leases():
    clock = Clock(1000.0)
    pool = LeasePool({"a": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
    pool.acquire("x")
    clock.advance(30.0)         # not yet expired (deadline 1060)
    assert pool.sweep_expired() == []
    assert pool.healthy_host_count() == 1


def test_sweep_quarantine_invalidates_sibling_leases():
    clock = Clock(1000.0)
    pool = LeasePool({"a": 2}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
    l1 = pool.acquire("x")
    clock.advance(30.0)
    l2 = pool.acquire("y")      # heartbeat 1030, deadline 1090
    clock.advance(40.0)         # now 1070: l1 (1060) expired, l2 (1090) not — same host
    affected = pool.sweep_expired()
    assert affected == ["a"]
    # the whole host was quarantined, so BOTH leases are gone
    assert pool.release(l1.token) is False
    assert pool.release(l2.token) is False
    assert pool.healthy_host_count() == 0


def test_heartbeat_keeps_lease_alive_past_old_deadline():
    clock = Clock(1000.0)
    pool = LeasePool({"a": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
    lease = pool.acquire("x")        # deadline 1060
    clock.advance(30.0)              # now 1030
    assert pool.heartbeat(lease.token) is True   # new deadline 1090
    clock.advance(45.0)              # now 1075: past old 1060, before new 1090
    assert pool.sweep_expired() == []            # heartbeat kept it alive


def test_heartbeat_after_expiry_does_not_resurrect():
    clock = Clock(1000.0)
    pool = LeasePool({"a": 1}, lease_ttl_s=60.0, now=clock, token_factory=_tokens())
    lease = pool.acquire("x")
    clock.advance(100.0)             # past expiry 1060
    pool.sweep_expired()             # lease gone, host quarantined
    assert pool.heartbeat(lease.token) is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_lease_pool_quarantine.py tests/unit/test_lease_pool_sweep.py -v`
Expected: FAIL with `AttributeError: ... 'quarantine'` / `'sweep_expired'`.

- [ ] **Step 3: Write the implementation (append to `LeasePool`)**

```python
    def quarantine(self, host: str) -> None:
        self._quarantined.add(host)
        for token in [t for t, ls in self._leases.items() if ls.host == host]:
            lease = self._leases.pop(token)
            self._used[lease.host].discard(lease.slot)

    def mark_reconciled(self, host: str) -> None:
        self._quarantined.discard(host)

    def sweep_expired(self) -> list[str]:
        now = self._now()
        affected = {ls.host for ls in self._leases.values() if ls.expiry_s < now}
        for host in affected:
            self.quarantine(host)
        return sorted(affected)
```

- [ ] **Step 4: Run the quarantine + sweep tests to verify they pass**

Run: `uv run pytest tests/unit/test_lease_pool_quarantine.py tests/unit/test_lease_pool_sweep.py -v`
Expected: all cases `passed` (the sweep file includes the two heartbeat-vs-expiry cases).

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/scheduling.py tests/unit/test_lease_pool_quarantine.py tests/unit/test_lease_pool_sweep.py
git commit -m "feat: add LeasePool quarantine/mark_reconciled/sweep_expired"
```

---

### Task 8: Phase 4a gate

**Files:**
- (no new source) — final full-toolchain verification.

**Interfaces:** none.

- [ ] **Step 1: Run the full suite**

Run: `uv run pytest -q`
Expected: all tests `passed` (Phases 1, 2, 3a, 3b + Phase 4a).

- [ ] **Step 2: Lint + type check**

Run: `uv run ruff check --fix . && uv run ruff check . && uv run mypy`
Expected: ruff auto-sorts then reports `All checks passed!`; mypy reports `Success: no issues found`.

If ruff/mypy flags anything in `scheduling.py`, fix it minimally (no behavior change, no config relaxation). In particular confirm the default `token_factory` lambda and the `Mapping`/`Iterable` annotations type-check under strict mypy.

- [ ] **Step 3: Commit (only if Step 2 made changes)**

```bash
git add -A
git commit -m "chore: phase 4a gate green (ruff + mypy)"
```

---

## Phase 4a self-review

Run before declaring Phase 4a done:

- [ ] `Lease` carries token, host, slot, attempt_id, expiry_s, heartbeat_s (§8.2) and validates them.
- [ ] `acquire` consumes a free slot, sets `expiry_s = now + ttl`, returns `None` when nothing free; honours exclusions and reuses an excluded host only once every healthy host has been tried (§7.1).
- [ ] `release` is token-checked and idempotent; `heartbeat` is token-checked, extends expiry, and never resurrects an expired lease (§8.2).
- [ ] `sweep_expired` quarantines hosts with past-deadline leases (not returns them to the pool) and reports them; `quarantine` invalidates every lease on the host; `mark_reconciled` restores it (§8.2).
- [ ] `healthy_host_count`/`free_slots` let the caller distinguish "wait" (capacity > 0, all busy) from "no capacity" (count == 0) — the basis for Phase 4b's `NoHealthyHostsError` (§8.2).
- [ ] All time/tokens come from injected `now`/`token_factory`; the pool has no Ray or SSH dependency.
- [ ] `uv run pytest -q`, `uv run ruff check .`, and `uv run mypy` all green.

**Deliverable:** `scheduling.py` — `Lease` + `LeasePool`, a pure clock-injected lease state machine, unit-tested end to end.

**Residuals carried to Phase 4b (documented):**
- The async `HostLease` Ray actor (`@ray.remote(num_cpus=0)`) wrapping `LeasePool` + `asyncio.Condition`: `acquire()` waits on the condition while `free_slots()==0 and healthy_host_count()>0`, and raises `NoHealthyHostsError` when `healthy_host_count()==0`; `release`/`heartbeat`/`sweep`/`mark_reconciled` notify the condition.
- `reconcile_host(transport, ...)`: reads remote runner state, terminates any orphaned process group (`ssh.terminate_process_group`), and on success calls `mark_reconciled`.
- The §3.2.6 stale-lock reconciliation: before a stale-lock takeover (Phase 3a `SessionLock`), probe for a live runner owned by the prior session.
- Holding the Phase 3b `ProvisioningOutcome.sessions` across execution and releasing at teardown (Dispatcher, Phase 6).
