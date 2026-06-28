# ray-dispatcher — Phase 3a: Digests + Session Lock — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the provisioning primitives — content digests for cache invalidation (`digests.py`) and an atomic, heartbeat-backed remote session lock (`locking.py`) — that Phase 3b's provisioning algorithm composes.

**Architecture:** `digests.py` is pure: it hashes the local source tree, the environment inputs, and the bundled runner into stable identifiers. `locking.py` drives a remote lock entirely through the Phase 2 `Transport` seam (so it is unit-testable with `FakeTransport` + an injected clock), using `sh -c` scripts that expand `$HOME` on the VM and `shlex.quote` for all data; the lock is acquired atomically via shell `noclobber`, refreshed by a background heartbeat, and only taken over from another session once its heartbeat has expired.

**Tech Stack:** Python ≥3.10 stdlib (`hashlib`, `os`, `json`, `shlex`, `threading`), the Phase 2 `Transport`, pytest/ruff/mypy.

**Spec:** `docs/superpowers/specs/2026-06-27-ray-dispatcher-design.md` (§6.2 digests, §6.3.1 lock acquisition, §3.2.6 session lock, §7 no-shell-from-user-strings). Phases 1+2 are on `main`.

## Global Constraints

Every task implicitly includes these. Values copied verbatim from the spec.

- `requires-python = ">=3.10"`; mypy runs `strict` over `src` only; ruff `select = E,F,I,UP,B`.
- **Digests (§6.2):**
  - `source_digest` covers, for every transferred source file after excludes: file path, mode, symlink target, and contents.
  - `environment_digest` covers `pyproject.toml`, `uv.lock`, the exact Python version, the exact uv version, dependency groups, sync flags, and the worker platform.
  - `runner_digest` covers the bundled remote supervisor (`remote_runner.py`).
  - **Secrets are intentionally excluded from all digests.**
- **Session lock (§3.2.6, §6.3.1):** acquired atomically before provisioning; a live lock held by another session raises `HostInUseError`. A stale lock is recoverable only after its heartbeat has expired. A host-side heartbeat keeps the lock live throughout provisioning and execution.
- **No shell from user strings (§7):** lock commands embed only library-controlled values — a generated session id, numeric timestamps, and fixed paths — and `shlex.quote` all data. (No `Job.command`/user string is ever involved in Phase 3a.)
- Digests are **stable** (deterministic for unchanged inputs) and change when any covered input changes.

## Module note

Spec §5 lists a single `provisioning.py` for this area. Phase 3 splits the digest and lock **primitives** into `digests.py` and `locking.py` (internal modules, not part of the public `__init__.py` surface) so that Phase 3b's `provisioning.py` stays focused on orchestration. This is an internal split for testability — no architecture change.

## Full-project file structure (only Phase 3a files are built here)

```text
src/ray_dispatcher/
├── errors.py / paths.py / models.py        # Phase 1 (on main)
├── ssh.py / remote_runner.py               # Phase 2 (on main)
├── digests.py        # THIS PLAN — source/environment/runner digests
├── locking.py        # THIS PLAN — atomic heartbeat session lock
├── provisioning.py   # Phase 3b — per-host algorithm + ProvisioningReport
├── scheduling.py     # Phase 4
├── results.py        # Phase 5
├── backends/…        # Phase 5-6
└── dispatcher.py     # Phase 6
```

### Phase 3a file structure

- Create: `src/ray_dispatcher/digests.py`
- Create: `src/ray_dispatcher/locking.py`
- Test: `tests/unit/test_digests_source.py`, `test_digests_env.py`, `test_digests_runner.py`, `test_session_lock_acquire.py`, `test_session_lock_contention.py`, `test_session_lock_heartbeat.py`, `test_heartbeat_thread.py`

---

### Task 1: `source_digest`

**Files:**
- Create: `src/ray_dispatcher/digests.py`
- Test: `tests/unit/test_digests_source.py`

**Interfaces:**
- Consumes: nothing from earlier phases.
- Produces:
  - `source_digest(root: str, excludes: Sequence[str]) -> str` — SHA-256 hex over the source tree after excludes.
  - internal `_excluded(rel: str, excludes: Sequence[str]) -> bool` and `_iter_source_files(root: Path, excludes: Sequence[str]) -> list[str]` (sorted POSIX relative paths; symlinks are recorded but never followed).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_digests_source.py`:

```python
import os
from pathlib import Path

from ray_dispatcher.digests import source_digest


def _tree(root: Path):
    (root / "pkg").mkdir()
    (root / "pkg" / "a.py").write_text("print('a')")
    (root / "run.py").write_text("print('run')")
    (root / ".venv").mkdir()
    (root / ".venv" / "junk").write_text("junk")


def test_source_digest_is_stable(tmp_path):
    _tree(tmp_path)
    d1 = source_digest(str(tmp_path), excludes=(".venv/",))
    d2 = source_digest(str(tmp_path), excludes=(".venv/",))
    assert d1 == d2
    assert len(d1) == 64  # sha256 hex


def test_excludes_are_applied(tmp_path):
    _tree(tmp_path)
    with_venv = source_digest(str(tmp_path), excludes=())
    without_venv = source_digest(str(tmp_path), excludes=(".venv/",))
    assert with_venv != without_venv


def test_content_change_changes_digest(tmp_path):
    _tree(tmp_path)
    before = source_digest(str(tmp_path), excludes=(".venv/",))
    (tmp_path / "run.py").write_text("print('CHANGED')")
    assert source_digest(str(tmp_path), excludes=(".venv/",)) != before


def test_mode_change_changes_digest(tmp_path):
    _tree(tmp_path)
    before = source_digest(str(tmp_path), excludes=(".venv/",))
    os.chmod(tmp_path / "run.py", 0o755)
    assert source_digest(str(tmp_path), excludes=(".venv/",)) != before


def test_symlink_target_is_recorded_not_followed(tmp_path):
    _tree(tmp_path)
    os.symlink("run.py", tmp_path / "link")
    before = source_digest(str(tmp_path), excludes=(".venv/",))
    os.unlink(tmp_path / "link")
    os.symlink("pkg/a.py", tmp_path / "link")  # same name, different target
    assert source_digest(str(tmp_path), excludes=(".venv/",)) != before
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_digests_source.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ray_dispatcher.digests'`.

- [ ] **Step 3: Write the implementation**

`src/ray_dispatcher/digests.py`:

```python
"""Content digests for cache invalidation (spec §6.2). Pure; reads local files."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Sequence
from pathlib import Path


def _excluded(rel: str, excludes: Sequence[str]) -> bool:
    # ponytail: path-prefix excludes (e.g. ".venv/"), not full rsync globs —
    #           sufficient for Project.exclude defaults.
    for raw in excludes:
        e = raw.rstrip("/")
        if rel == e or rel.startswith(e + "/"):
            return True
    return False


def _iter_source_files(root: Path, excludes: Sequence[str]) -> list[str]:
    results: list[str] = []

    def rec(directory: Path, prefix: str) -> None:
        for entry in sorted(os.scandir(directory), key=lambda e: e.name):
            rel = f"{prefix}{entry.name}"
            if _excluded(rel, excludes):
                continue
            if entry.is_symlink():
                results.append(rel)
            elif entry.is_dir():
                rec(Path(entry.path), rel + "/")
            elif entry.is_file():
                results.append(rel)

    rec(root, "")
    return results


def source_digest(root: str, excludes: Sequence[str]) -> str:
    root_path = Path(root)
    h = hashlib.sha256()
    for rel in _iter_source_files(root_path, excludes):
        p = root_path / rel
        h.update(rel.encode())
        h.update(b"\0")
        if p.is_symlink():
            h.update(b"L")
            h.update(os.readlink(p).encode())
        else:
            mode = p.stat().st_mode & 0o777
            h.update(f"M{mode:o}".encode())
            h.update(p.read_bytes())
        h.update(b"\0")
    return h.hexdigest()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_digests_source.py -v`
Expected: all cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/digests.py tests/unit/test_digests_source.py
git commit -m "feat: add source_digest for cache invalidation"
```

---

### Task 2: `environment_digest`

**Files:**
- Modify: `src/ray_dispatcher/digests.py` (append)
- Test: `tests/unit/test_digests_env.py`

**Interfaces:**
- Consumes: `Project` (Phase 1 models).
- Produces: `environment_digest(project: Project, *, platform: str, sync_flags: Sequence[str]) -> str` — hashes `pyproject.toml` + `uv.lock` (read from `project.path`) plus `project.python`, `project.uv_version`, `project.dependency_groups`, `sync_flags`, and `platform`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_digests_env.py`:

```python
from pathlib import Path

import pytest

from ray_dispatcher.digests import environment_digest
from ray_dispatcher.models import Project


def _project(tmp_path: Path, **over) -> Project:
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n")
    (tmp_path / "uv.lock").write_text("# lock v1\n")
    kwargs = dict(path=str(tmp_path), project_id="x", python="3.10.18",
                  uv_version="0.11.25")
    kwargs.update(over)
    return Project(**kwargs)


def test_env_digest_stable(tmp_path):
    p = _project(tmp_path)
    a = environment_digest(p, platform="linux-x86_64", sync_flags=("--locked",))
    b = environment_digest(p, platform="linux-x86_64", sync_flags=("--locked",))
    assert a == b and len(a) == 64


@pytest.mark.parametrize("mutate", [
    lambda tmp, p: (tmp / "uv.lock").write_text("# lock v2\n") or p,
    lambda tmp, p: (tmp / "pyproject.toml").write_text("[project]\nname='y'\n") or p,
])
def test_env_digest_changes_on_file_change(tmp_path, mutate):
    p = _project(tmp_path)
    before = environment_digest(p, platform="linux-x86_64", sync_flags=("--locked",))
    p = mutate(tmp_path, p)
    assert environment_digest(p, platform="linux-x86_64", sync_flags=("--locked",)) != before


def test_env_digest_changes_on_metadata(tmp_path):
    p = _project(tmp_path)
    base = environment_digest(p, platform="linux-x86_64", sync_flags=("--locked",))
    p2 = _project(tmp_path, python="3.10.19")
    assert environment_digest(p2, platform="linux-x86_64", sync_flags=("--locked",)) != base
    assert environment_digest(p, platform="linux-aarch64", sync_flags=("--locked",)) != base
    assert environment_digest(p, platform="linux-x86_64", sync_flags=("--frozen",)) != base
    p3 = _project(tmp_path, dependency_groups=("dev",))
    assert environment_digest(p3, platform="linux-x86_64", sync_flags=("--locked",)) != base
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_digests_env.py -v`
Expected: FAIL with `ImportError: cannot import name 'environment_digest'`.

- [ ] **Step 3: Write the implementation (append to `digests.py`)**

Add to the imports block of `digests.py`:

```python
from .models import Project
```

Append to `digests.py`:

```python
def environment_digest(
    project: Project, *, platform: str, sync_flags: Sequence[str]
) -> str:
    base = Path(project.path)
    h = hashlib.sha256()
    for fname in ("pyproject.toml", "uv.lock"):
        h.update((base / fname).read_bytes())
        h.update(b"\0")
    for field in (
        project.python,
        project.uv_version,
        "\0".join(project.dependency_groups),
        "\0".join(sync_flags),
        platform,
    ):
        h.update(field.encode())
        h.update(b"\0")
    return h.hexdigest()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_digests_env.py -v`
Expected: all cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/digests.py tests/unit/test_digests_env.py
git commit -m "feat: add environment_digest covering lock, versions, flags, platform"
```

---

### Task 3: `runner_digest`

**Files:**
- Modify: `src/ray_dispatcher/digests.py` (append)
- Test: `tests/unit/test_digests_runner.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `runner_digest(runner_path: str) -> str` — SHA-256 hex of the runner file's bytes.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_digests_runner.py`:

```python
from ray_dispatcher.digests import runner_digest


def test_runner_digest_hashes_contents(tmp_path):
    f = tmp_path / "remote_runner.py"
    f.write_text("print('v1')")
    before = runner_digest(str(f))
    assert len(before) == 64
    f.write_text("print('v2')")
    assert runner_digest(str(f)) != before


def test_runner_digest_matches_real_runner():
    # the bundled runner exists and hashes without error
    d = runner_digest("src/ray_dispatcher/remote_runner.py")
    assert len(d) == 64
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_digests_runner.py -v`
Expected: FAIL with `ImportError: cannot import name 'runner_digest'`.

- [ ] **Step 3: Write the implementation (append to `digests.py`)**

```python
def runner_digest(runner_path: str) -> str:
    return hashlib.sha256(Path(runner_path).read_bytes()).hexdigest()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_digests_runner.py -v`
Expected: both cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/digests.py tests/unit/test_digests_runner.py
git commit -m "feat: add runner_digest"
```

---

### Task 4: `SessionLock` — payload/read/write + atomic acquire (happy path)

**Files:**
- Create: `src/ray_dispatcher/locking.py`
- Test: `tests/unit/test_session_lock_acquire.py`

**Interfaces:**
- Consumes: `Transport` (Phase 2 `ssh.py`), `HostInUseError` (Phase 1 `errors.py`).
- Produces:
  - `SessionLock(transport, session_id, *, ttl_s=60.0, now=time.time)` with `acquire()`, `heartbeat()`, `release()` (heartbeat/release land in Task 6), and internals `_payload()`, `_read_owner() -> dict[str, Any] | None`, `_write_owner()`.
  - module helper `_sh(script: str) -> list[str]` returning `["sh", "-c", script]`.
- All remote operations go through `transport.run`. Paths use `"$HOME/.ray_dispatcher/locks/…"` inside `sh -c` so the VM shell expands `$HOME`; data is `shlex.quote`d.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_session_lock_acquire.py`:

```python
from ray_dispatcher.ssh import CommandResult, FakeTransport
from ray_dispatcher.locking import SessionLock


def _script(call):
    # call is ("run", ("sh", "-c", script)); return the script
    return call[1][2]


def test_acquire_creates_lock_atomically():
    # the noclobber create (set -C) succeeds -> we own the lock, no takeover
    def results(argv):
        return CommandResult(0, "", "", 0.0)  # mkdir + create both succeed

    t = FakeTransport(run_results=results)
    SessionLock(t, "sess-1").acquire()
    scripts = [_script(c) for c in t.calls if c[0] == "run"]
    assert any("mkdir -p" in s for s in scripts)
    assert any("set -C" in s and "session.json" in s for s in scripts)
    # happy path does not read or overwrite an existing owner
    assert not any("mv -f" in s for s in scripts)


def test_payload_contains_session_and_heartbeat():
    t = FakeTransport()
    lock = SessionLock(t, "sess-9", now=lambda: 1234.5)
    import json
    payload = json.loads(lock._payload())
    assert payload == {"session_id": "sess-9", "heartbeat": 1234.5}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_session_lock_acquire.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ray_dispatcher.locking'`.

- [ ] **Step 3: Write the implementation**

`src/ray_dispatcher/locking.py`:

```python
"""Atomic, heartbeat-backed remote session lock (spec §3.2.6, §6.3.1).

Drives a remote lock entirely through the Phase 2 Transport seam. Every command
is an ``sh -c`` script so the VM shell expands ``$HOME``; all data is shlex-quoted.
The lock is one file, created atomically via shell ``noclobber`` (``set -C``).
"""

from __future__ import annotations

import json
import shlex
import threading
import time
from collections.abc import Callable
from typing import Any

from .errors import HostInUseError
from .ssh import Transport

_LOCK_DIR = '"$HOME/.ray_dispatcher/locks"'
_LOCK_FILE = '"$HOME/.ray_dispatcher/locks/session.json"'
_LOCK_TMP = '"$HOME/.ray_dispatcher/locks/.session.json.tmp"'


def _sh(script: str) -> list[str]:
    return ["sh", "-c", script]


class SessionLock:
    def __init__(
        self,
        transport: Transport,
        session_id: str,
        *,
        ttl_s: float = 60.0,
        now: Callable[[], float] = time.time,
    ) -> None:
        self.transport = transport
        self.session_id = session_id
        self.ttl_s = ttl_s
        self.now = now

    def _payload(self) -> str:
        return json.dumps({"session_id": self.session_id, "heartbeat": self.now()})

    def _read_owner(self) -> dict[str, Any] | None:
        result = self.transport.run(_sh(f"cat {_LOCK_FILE} 2>/dev/null"))
        if result.returncode != 0 or not result.stdout.strip():
            return None
        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError:
            return None
        return data if isinstance(data, dict) else None

    def _write_owner(self) -> None:
        owner = shlex.quote(self._payload())
        self.transport.run(
            _sh(f"printf %s {owner} > {_LOCK_TMP} && mv -f {_LOCK_TMP} {_LOCK_FILE}")
        )

    def acquire(self) -> None:
        self.transport.run(_sh(f"mkdir -p {_LOCK_DIR}"))
        owner = shlex.quote(self._payload())
        created = self.transport.run(_sh(f"set -C; printf %s {owner} > {_LOCK_FILE}"))
        if created.returncode == 0:
            return  # created atomically -> we own it
        existing = self._read_owner()
        if existing is None or existing.get("session_id") == self.session_id:
            self._write_owner()  # ours, or unreadable/corrupt -> take it
            return
        if self.now() - float(existing.get("heartbeat", 0)) > self.ttl_s:
            self._write_owner()  # stale: heartbeat expired -> take over
            return
        raise HostInUseError(
            f"session lock held by {existing.get('session_id')!r}"
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_session_lock_acquire.py -v`
Expected: both cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/locking.py tests/unit/test_session_lock_acquire.py
git commit -m "feat: add SessionLock with atomic acquire (noclobber)"
```

---

### Task 5: `SessionLock.acquire` — contention (live / ours / stale)

**Files:**
- Test: `tests/unit/test_session_lock_contention.py`

**Interfaces:**
- Consumes: `SessionLock` (Task 4), `HostInUseError`, `FakeTransport`/`CommandResult`.
- Produces: no new code — this task adds tests that pin the contention branches already implemented in Task 4. (If a branch is wrong, fix `acquire` minimally and note it.)

- [ ] **Step 1: Write the failing test**

`tests/unit/test_session_lock_contention.py`:

```python
import json

import pytest

from ray_dispatcher.errors import HostInUseError
from ray_dispatcher.ssh import CommandResult, FakeTransport
from ray_dispatcher.locking import SessionLock


def _responder(create_rc, owner):
    """FakeTransport callback: mkdir ok; create returns create_rc; cat returns owner json."""
    owner_json = json.dumps(owner) if owner is not None else ""

    def results(argv):
        script = argv[2]
        if "set -C" in script:
            return CommandResult(create_rc, "", "", 0.0)
        if script.startswith("cat "):
            rc = 0 if owner_json else 1
            return CommandResult(rc, owner_json, "", 0.0)
        return CommandResult(0, "", "", 0.0)  # mkdir, mv

    return results


def _scripts(t):
    return [c[1][2] for c in t.calls if c[0] == "run"]


def test_live_lock_from_other_session_raises():
    t = FakeTransport(_responder(create_rc=1,
                                 owner={"session_id": "other", "heartbeat": 1000.0}))
    lock = SessionLock(t, "mine", ttl_s=60.0, now=lambda: 1000.0)  # age 0 < ttl
    with pytest.raises(HostInUseError):
        lock.acquire()
    assert not any("mv -f" in s for s in _scripts(t))  # never took it over


def test_stale_lock_is_taken_over():
    t = FakeTransport(_responder(create_rc=1,
                                 owner={"session_id": "other", "heartbeat": 900.0}))
    lock = SessionLock(t, "mine", ttl_s=60.0, now=lambda: 1000.0)  # age 100 > ttl
    lock.acquire()  # no raise
    assert any("mv -f" in s for s in _scripts(t))  # took it over


def test_own_lock_is_refreshed():
    t = FakeTransport(_responder(create_rc=1,
                                 owner={"session_id": "mine", "heartbeat": 1000.0}))
    lock = SessionLock(t, "mine", ttl_s=60.0, now=lambda: 1000.0)
    lock.acquire()  # no raise
    assert any("mv -f" in s for s in _scripts(t))
```

- [ ] **Step 2: Run test to verify it passes (branches already implemented in Task 4)**

Run: `uv run pytest tests/unit/test_session_lock_contention.py -v`
Expected: all three cases `passed`. If any fails, the corresponding branch in `SessionLock.acquire` is wrong — fix it minimally and re-run.

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_session_lock_contention.py
git commit -m "test: pin SessionLock acquire contention branches"
```

---

### Task 6: `SessionLock.heartbeat` + `release`

**Files:**
- Modify: `src/ray_dispatcher/locking.py` (append two methods)
- Test: `tests/unit/test_session_lock_heartbeat.py`

**Interfaces:**
- Consumes: `SessionLock` (Task 4).
- Produces: `SessionLock.heartbeat()` (refreshes the owner file only if we still own it) and `SessionLock.release()` (removes the lock file only if we own it).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_session_lock_heartbeat.py`:

```python
import json

from ray_dispatcher.ssh import CommandResult, FakeTransport
from ray_dispatcher.locking import SessionLock


def _owner_responder(owner):
    owner_json = json.dumps(owner)

    def results(argv):
        if argv[2].startswith("cat "):
            return CommandResult(0, owner_json, "", 0.0)
        return CommandResult(0, "", "", 0.0)

    return results


def _scripts(t):
    return [c[1][2] for c in t.calls if c[0] == "run"]


def test_heartbeat_refreshes_when_owned():
    t = FakeTransport(_owner_responder({"session_id": "mine", "heartbeat": 1.0}))
    SessionLock(t, "mine").heartbeat()
    assert any("mv -f" in s for s in _scripts(t))


def test_heartbeat_noop_when_not_owned():
    t = FakeTransport(_owner_responder({"session_id": "other", "heartbeat": 1.0}))
    SessionLock(t, "mine").heartbeat()
    assert not any("mv -f" in s for s in _scripts(t))


def test_release_removes_when_owned():
    t = FakeTransport(_owner_responder({"session_id": "mine", "heartbeat": 1.0}))
    SessionLock(t, "mine").release()
    assert any("rm -f" in s for s in _scripts(t))


def test_release_noop_when_not_owned():
    t = FakeTransport(_owner_responder({"session_id": "other", "heartbeat": 1.0}))
    SessionLock(t, "mine").release()
    assert not any("rm -f" in s for s in _scripts(t))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_session_lock_heartbeat.py -v`
Expected: FAIL with `AttributeError: 'SessionLock' object has no attribute 'heartbeat'`.

- [ ] **Step 3: Write the implementation (append two methods to `SessionLock`)**

```python
    def heartbeat(self) -> None:
        existing = self._read_owner()
        if existing is not None and existing.get("session_id") == self.session_id:
            self._write_owner()

    def release(self) -> None:
        existing = self._read_owner()
        if existing is not None and existing.get("session_id") == self.session_id:
            self.transport.run(_sh(f"rm -f {_LOCK_FILE}"))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_session_lock_heartbeat.py -v`
Expected: all four cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/locking.py tests/unit/test_session_lock_heartbeat.py
git commit -m "feat: add SessionLock heartbeat and release (ownership-checked)"
```

---

### Task 7: `HeartbeatThread`

**Files:**
- Modify: `src/ray_dispatcher/locking.py` (append)
- Test: `tests/unit/test_heartbeat_thread.py`

**Interfaces:**
- Consumes: `SessionLock` (Tasks 4/6).
- Produces: `HeartbeatThread(lock: SessionLock, interval_s: float)` — a daemon thread that calls `lock.heartbeat()` every `interval_s` until `stop()`. Heartbeat exceptions are swallowed (best-effort; the lock's TTL is the real safety net).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_heartbeat_thread.py`:

```python
import json
import time

from ray_dispatcher.ssh import CommandResult, FakeTransport
from ray_dispatcher.locking import HeartbeatThread, SessionLock


def test_heartbeat_thread_beats_then_stops():
    owner = json.dumps({"session_id": "mine", "heartbeat": 1.0})

    def results(argv):
        if argv[2].startswith("cat "):
            return CommandResult(0, owner, "", 0.0)
        return CommandResult(0, "", "", 0.0)

    t = FakeTransport(run_results=results)
    hb = HeartbeatThread(SessionLock(t, "mine"), interval_s=0.01)
    hb.start()
    time.sleep(0.05)
    hb.stop()
    refreshes = sum(1 for c in t.calls if c[0] == "run" and "mv -f" in c[1][2])
    assert refreshes >= 1
    # after stop, no further beats
    settled = refreshes
    time.sleep(0.03)
    assert sum(1 for c in t.calls if c[0] == "run" and "mv -f" in c[1][2]) == settled
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_heartbeat_thread.py -v`
Expected: FAIL with `ImportError: cannot import name 'HeartbeatThread'`.

- [ ] **Step 3: Write the implementation (append to `locking.py`)**

```python
class HeartbeatThread(threading.Thread):
    """Background daemon that refreshes a SessionLock until stopped."""

    def __init__(self, lock: SessionLock, interval_s: float) -> None:
        super().__init__(daemon=True)
        self._lock = lock
        self._interval = interval_s
        self._stopped = threading.Event()

    def run(self) -> None:
        while not self._stopped.wait(self._interval):
            try:
                self._lock.heartbeat()
            except Exception:  # noqa: BLE001 — heartbeat is best-effort; TTL is the net
                pass

    def stop(self) -> None:
        self._stopped.set()
        self.join(timeout=5)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_heartbeat_thread.py -v`
Expected: `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/locking.py tests/unit/test_heartbeat_thread.py
git commit -m "feat: add HeartbeatThread to keep the session lock live"
```

---

### Task 8: Phase 3a gate

**Files:**
- (no new source) — final full-toolchain verification.

**Interfaces:** none.

- [ ] **Step 1: Run the full suite**

Run: `uv run pytest -q`
Expected: all tests `passed` (Phases 1+2 + Phase 3a).

- [ ] **Step 2: Lint + type check**

Run: `uv run ruff check --fix . && uv run ruff check . && uv run mypy`
Expected: ruff auto-sorts then reports `All checks passed!`; mypy reports `Success: no issues found`.

If ruff/mypy flags anything in `digests.py`/`locking.py`, fix it minimally (no behavior change, no config relaxation, no blanket `# type: ignore`).

- [ ] **Step 3: Commit (only if Step 2 made changes)**

```bash
git add -A
git commit -m "chore: phase 3a gate green (ruff + mypy)"
```

---

## Phase 3a self-review

Run before declaring Phase 3a done:

- [ ] `source_digest` covers path + mode + symlink-target + contents and honors excludes; `environment_digest` covers pyproject + uv.lock + python + uv_version + dependency_groups + sync_flags + platform; `runner_digest` hashes the runner file. (§6.2)
- [ ] No secret value is read by any digest function (secrets live elsewhere; digests only touch source tree / pyproject / uv.lock / runner). (§6.2)
- [ ] `SessionLock.acquire` creates atomically (`set -C`), raises `HostInUseError` on a live foreign lock, takes over only when the heartbeat age exceeds `ttl_s`, and refreshes its own lock. (§3.2.6, §6.3.1)
- [ ] Lock commands embed only the generated `session_id`, numeric timestamps, and fixed `$HOME`-relative paths, with `shlex.quote` on the JSON payload — no user string anywhere. (§7)
- [ ] `HeartbeatThread` beats until `stop()` and swallows beat errors.
- [ ] `uv run pytest -q`, `uv run ruff check .`, and `uv run mypy` all green.

**Deliverable:** `digests.py` (cache-invalidation identifiers) and `locking.py` (atomic heartbeat session lock), unit-tested with temp dirs, `FakeTransport`, and an injected clock — the primitives Phase 3b's `provisioning.py` composes. **Residual race (documented):** the stale-lock takeover is bounded by the heartbeat TTL; the spec's full reconciliation (probe for a live runner before takeover) is wired in Phase 4 — note this for 3b/4.
