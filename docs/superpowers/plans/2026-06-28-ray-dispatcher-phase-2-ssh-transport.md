# ray-dispatcher — Phase 2: SSH Transport + Remote Runner — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the host-side SSH/rsync transport (`ssh.py`) and the VM-side standalone subprocess supervisor (`remote_runner.py`) that later phases use to provision, run, and control remote jobs.

**Architecture:** `ssh.py` runs on the host: it resolves+validates SSH config from a `RemoteHost`, exposes a `Transport` seam (Fabric for `run`, OpenSSH `rsync` for `push`/`pull`) plus a `FakeTransport` for tests, and a host-side process-group terminator. `remote_runner.py` is a self-contained stdlib-only script that runs on the VM: it reads a JSON manifest, launches the job with `subprocess.Popen(..., start_new_session=True)`, records PID/PGID, tees output to remote files while forwarding it live, and writes a result file. No user string is ever handed to a remote shell — the job argv runs via `Popen`.

**Tech Stack:** Python ≥3.10, Fabric (+paramiko), OpenSSH `rsync`, stdlib `subprocess`/`threading`, pytest/ruff/mypy.

**Spec:** `docs/superpowers/specs/2026-06-27-ray-dispatcher-design.md` (§3.1, §4.1, §5, §7, §8.1, §11, §13). Phase 1 (`errors.py`, `paths.py`, `models.py`, exports) is already on `main`.

## Global Constraints

Every task implicitly includes these. Values copied verbatim from the spec.

- `requires-python = ">=3.10"`; mypy runs `strict` over `src` only (Phase 1 config).
- **Module boundaries (§5):** Only `ssh.py` constructs Fabric connections or rsync invocations. Only `remote_runner.py` creates the remote application subprocess.
- **`remote_runner.py` is standalone:** stdlib only, **no imports from `ray_dispatcher`** — it runs in the project venv on a VM where the package is absent (§7, §13).
- **SSH security (§3.1, §4.1, §13):** host-key checking is always enabled; no password SSH. Fabric and rsync receive the **same** resolved SSH identity, port, user, and known-hosts settings. Missing `identity_file` or `known_hosts_file` → `ModelValidationError`.
- **Runner protocol (§7):** `subprocess.Popen(argv, cwd=..., env=..., start_new_session=True)`; record PID and process-group ID; forward stdout/stderr over SSH **while also keeping remote copies**; binary/undecodable output preserved with replacement markers; log-streaming failure must not hide the remote exit status. The runner prepends the shared `.venv/bin` to `PATH`, sets `VIRTUAL_ENV`, and sets declared secret variables. It **never invokes `uv run`**.
- **No shell from user strings (§7 step 5):** the job's argv runs via `Popen` (no shell). `ssh.py`'s `run` only ever shells argvs the library controls (the runner invocation, `kill`, `mkdir`, `rsync`, `uv …`).
- **Process-group termination (§8.1):** SIGTERM to the recorded process group, bounded grace, then SIGKILL; complete only after a probe confirms the group is gone. Closing the original SSH channel is never treated as process termination.

## Full-project file structure (only Phase 2 files are built here)

```text
src/ray_dispatcher/
├── errors.py            # Phase 1 (on main)
├── paths.py             # Phase 1 (on main)
├── models.py            # Phase 1 (on main)
├── ssh.py               # THIS PHASE — SshConfig, Transport, SshTransport, FakeTransport, terminator
├── remote_runner.py     # THIS PHASE — standalone VM-side subprocess supervisor
├── provisioning.py      # Phase 3
├── scheduling.py        # Phase 4
├── results.py           # Phase 5
├── backends/…           # Phase 5-6
└── dispatcher.py        # Phase 6
```

### Phase 2 file structure

- Create: `src/ray_dispatcher/ssh.py`
- Create: `src/ray_dispatcher/remote_runner.py`
- Modify: `pyproject.toml` (mypy override for the untyped SSH libraries — Task 5)
- Test: `tests/unit/test_ssh_config.py`, `test_transport_fake.py`, `test_rsync_argv.py`, `test_ssh_transport.py`, `test_ssh_connection.py`, `test_remote_runner.py`, `test_remote_runner_forward.py`, `test_terminate_pgroup.py`

`ssh.py` and `remote_runner.py` are **internal** modules — they are not added to the public `__init__.py` exports (the public API is the §4 value objects from Phase 1).

---

### Task 1: `SshConfig` — resolve and validate SSH settings

Closes the Phase-1-deferred file-existence validation (§4.1). Produces one resolved, validated config object that both Fabric and rsync consume identically.

**Files:**
- Create: `src/ray_dispatcher/ssh.py`
- Test: `tests/unit/test_ssh_config.py`

**Interfaces:**
- Consumes: `RemoteHost` and `ModelValidationError` (Phase 1).
- Produces: `SshConfig(host, user, port, identity_file: str | None, known_hosts_file: str)` frozen; `SshConfig.from_host(host: RemoteHost) -> SshConfig` (expands `~`, makes paths absolute, validates existence).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_ssh_config.py`:

```python
import pytest

from ray_dispatcher.errors import ModelValidationError
from ray_dispatcher.models import RemoteHost
from ray_dispatcher.ssh import SshConfig


def _host(tmp_path, **over):
    kh = tmp_path / "known_hosts"
    kh.write_text("")
    kwargs = dict(host="10.0.0.5", user="ubuntu", known_hosts_file=str(kh))
    kwargs.update(over)
    return RemoteHost(**kwargs)


def test_from_host_resolves_paths(tmp_path):
    idf = tmp_path / "id_ed25519"
    idf.write_text("key")
    cfg = SshConfig.from_host(_host(tmp_path, identity_file=str(idf)))
    assert cfg.host == "10.0.0.5"
    assert cfg.user == "ubuntu"
    assert cfg.port == 22
    assert cfg.identity_file == str(idf.resolve())
    assert cfg.known_hosts_file.endswith("known_hosts")


def test_from_host_allows_no_identity_uses_agent(tmp_path):
    cfg = SshConfig.from_host(_host(tmp_path))  # identity_file=None
    assert cfg.identity_file is None


def test_from_host_rejects_missing_identity(tmp_path):
    with pytest.raises(ModelValidationError):
        SshConfig.from_host(_host(tmp_path, identity_file=str(tmp_path / "nope")))


def test_from_host_rejects_missing_known_hosts(tmp_path):
    h = RemoteHost(host="h", user="u", known_hosts_file=str(tmp_path / "absent"))
    with pytest.raises(ModelValidationError):
        SshConfig.from_host(h)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_ssh_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ray_dispatcher.ssh'`.

- [ ] **Step 3: Write the implementation**

`src/ray_dispatcher/ssh.py`:

```python
"""Host-side SSH/rsync transport and shared SSH options (spec §3.1, §4.1, §5)."""

from __future__ import annotations

import os
from dataclasses import dataclass

from .errors import ModelValidationError
from .models import RemoteHost


def _resolve_existing(path: str, label: str) -> str:
    resolved = os.path.abspath(os.path.expanduser(path))
    if not os.path.isfile(resolved):
        raise ModelValidationError(f"{label} not found: {path}")
    return resolved


@dataclass(frozen=True)
class SshConfig:
    host: str
    user: str
    port: int
    identity_file: str | None
    known_hosts_file: str

    @classmethod
    def from_host(cls, host: RemoteHost) -> "SshConfig":
        known_hosts = _resolve_existing(host.known_hosts_file, "known_hosts file")
        identity = (
            _resolve_existing(host.identity_file, "identity file")
            if host.identity_file is not None
            else None
        )
        return cls(
            host=host.host,
            user=host.user,
            port=host.port,
            identity_file=identity,
            known_hosts_file=known_hosts,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_ssh_config.py -v`
Expected: all cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/ssh.py tests/unit/test_ssh_config.py
git commit -m "feat: add SshConfig with resolved, validated SSH settings"
```

---

### Task 2: `CommandResult`, `Transport` seam, `TransportError`, `FakeTransport`

**Files:**
- Modify: `src/ray_dispatcher/ssh.py` (append)
- Test: `tests/unit/test_transport_fake.py`

**Interfaces:**
- Consumes: `DispatcherError` (Phase 1).
- Produces:
  - `CommandResult(returncode: int, stdout: str, stderr: str, duration_s: float)` frozen, with `.ok -> bool`.
  - `TransportError(DispatcherError)` — uniform failure type of the Transport seam.
  - `Transport(Protocol)`: `run(argv, *, timeout_s=None) -> CommandResult`; `push(local, remote, *, delete=False, excludes=()) -> None`; `pull(remote, local, *, delete=False, excludes=()) -> None`.
  - `FakeTransport(run_results: Callable[[list[str]], CommandResult] | None = None)` with a `.calls: list[tuple]` record, satisfying `Transport`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_transport_fake.py`:

```python
from ray_dispatcher.ssh import CommandResult, FakeTransport


def test_command_result_ok():
    assert CommandResult(0, "", "", 0.1).ok is True
    assert CommandResult(1, "", "boom", 0.1).ok is False


def test_fake_records_calls_and_returns_default_ok():
    t = FakeTransport()
    r = t.run(["echo", "hi"])
    t.push("/local", "/remote", delete=True, excludes=(".git/",))
    t.pull("/remote", "/local")
    assert r.ok
    assert t.calls[0] == ("run", ("echo", "hi"))
    assert t.calls[1] == ("push", ("/local", "/remote", True, (".git/",)))
    assert t.calls[2] == ("pull", ("/remote", "/local", False, ()))


def test_fake_run_results_callback():
    def results(argv):
        return CommandResult(0 if argv[0] == "true" else 7, "", "", 0.0)

    t = FakeTransport(run_results=results)
    assert t.run(["true"]).returncode == 0
    assert t.run(["false"]).returncode == 7
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_transport_fake.py -v`
Expected: FAIL with `ImportError: cannot import name 'FakeTransport'`.

- [ ] **Step 3: Write the implementation (append to `ssh.py`)**

Add to the imports block of `ssh.py`:

```python
from collections.abc import Callable, Sequence
from typing import Protocol

from .errors import DispatcherError, ModelValidationError
```

(Replace the existing `from .errors import ModelValidationError` line with the combined import above.)

Append to `ssh.py`:

```python
class TransportError(DispatcherError):
    """A transport (ssh/rsync) operation failed. Phase 5 maps this to FailureKind.SSH."""


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str
    duration_s: float

    @property
    def ok(self) -> bool:
        return self.returncode == 0


class Transport(Protocol):
    def run(self, argv: Sequence[str], *, timeout_s: float | None = None) -> CommandResult: ...

    def push(
        self, local: str, remote: str, *, delete: bool = False, excludes: Sequence[str] = ()
    ) -> None: ...

    def pull(
        self, remote: str, local: str, *, delete: bool = False, excludes: Sequence[str] = ()
    ) -> None: ...


class FakeTransport:
    """In-memory Transport for unit tests. Records calls; programmable run results."""

    def __init__(self, run_results: Callable[[list[str]], CommandResult] | None = None) -> None:
        self.calls: list[tuple[object, ...]] = []
        self._run_results = run_results

    def run(self, argv: Sequence[str], *, timeout_s: float | None = None) -> CommandResult:
        self.calls.append(("run", tuple(argv)))
        if self._run_results is not None:
            return self._run_results(list(argv))
        return CommandResult(0, "", "", 0.0)

    def push(
        self, local: str, remote: str, *, delete: bool = False, excludes: Sequence[str] = ()
    ) -> None:
        self.calls.append(("push", (local, remote, delete, tuple(excludes))))

    def pull(
        self, remote: str, local: str, *, delete: bool = False, excludes: Sequence[str] = ()
    ) -> None:
        self.calls.append(("pull", (remote, local, delete, tuple(excludes))))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_transport_fake.py -v`
Expected: all cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/ssh.py tests/unit/test_transport_fake.py
git commit -m "feat: add CommandResult, Transport seam, and FakeTransport"
```

---

### Task 3: `build_rsync_argv` — pure rsync command builder

**Files:**
- Modify: `src/ray_dispatcher/ssh.py` (append)
- Test: `tests/unit/test_rsync_argv.py`

**Interfaces:**
- Consumes: `SshConfig` (Task 1).
- Produces: `build_rsync_argv(cfg: SshConfig, src: str, dst: str, *, delete: bool, excludes: Sequence[str]) -> list[str]` — builds an `rsync -a` argv whose `-e` option carries the **same** SSH settings (port, identity, known-hosts, strict host-key checking) used by Fabric. The caller (Task 4) forms the `user@host:` remote side.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_rsync_argv.py`:

```python
from ray_dispatcher.ssh import SshConfig, build_rsync_argv


def _cfg():
    return SshConfig(host="h", user="u", port=2222,
                     identity_file="/keys/id", known_hosts_file="/keys/kh")


def test_argv_has_archive_and_ssh_options():
    argv = build_rsync_argv(_cfg(), "/local/", "u@h:/remote/", delete=False, excludes=())
    assert argv[0] == "rsync"
    assert "-a" in argv
    # the -e value is one argument holding the ssh invocation
    e_index = argv.index("-e")
    ssh_opt = argv[e_index + 1]
    assert ssh_opt.startswith("ssh ")
    assert "-p 2222" in ssh_opt
    assert "-i /keys/id" in ssh_opt
    assert "UserKnownHostsFile=/keys/kh" in ssh_opt
    assert "StrictHostKeyChecking=yes" in ssh_opt
    assert argv[-2:] == ["/local/", "u@h:/remote/"]


def test_argv_delete_and_excludes():
    argv = build_rsync_argv(_cfg(), "/a", "/b", delete=True, excludes=(".git/", "solutions/"))
    assert "--delete" in argv
    assert argv.count("--exclude") == 2
    i = argv.index("--exclude")
    assert argv[i + 1] == ".git/"


def test_argv_no_identity_omits_i():
    cfg = SshConfig(host="h", user="u", port=22, identity_file=None,
                    known_hosts_file="/kh")
    joined = " ".join(build_rsync_argv(cfg, "/a", "/b", delete=False, excludes=()))
    assert "-i " not in joined
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_rsync_argv.py -v`
Expected: FAIL with `ImportError: cannot import name 'build_rsync_argv'`.

- [ ] **Step 3: Write the implementation (append to `ssh.py`)**

Add to the imports block of `ssh.py`:

```python
import shlex
```

Append to `ssh.py`:

```python
def _ssh_e_option(cfg: SshConfig) -> str:
    """The rsync ``-e`` value: an ssh invocation carrying the same SSH settings
    as Fabric. shlex.join keeps paths with spaces safe."""
    parts = [
        "ssh",
        "-p", str(cfg.port),
        "-o", f"UserKnownHostsFile={cfg.known_hosts_file}",
        "-o", "StrictHostKeyChecking=yes",
    ]
    if cfg.identity_file:
        parts += ["-i", cfg.identity_file]
    return shlex.join(parts)


def build_rsync_argv(
    cfg: SshConfig, src: str, dst: str, *, delete: bool, excludes: Sequence[str]
) -> list[str]:
    argv = ["rsync", "-a", "--protect-args", "-e", _ssh_e_option(cfg)]
    if delete:
        argv.append("--delete")
    for ex in excludes:
        argv += ["--exclude", ex]
    argv += [src, dst]
    return argv
```

Note: the test asserts substrings like `-p 2222` and `-i /keys/id`. `shlex.join` only quotes when needed; these unquoted tokens appear verbatim, so the substring assertions hold for paths without spaces.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_rsync_argv.py -v`
Expected: all cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/ssh.py tests/unit/test_rsync_argv.py
git commit -m "feat: add pure rsync argv builder with shared ssh options"
```

---

### Task 4: `SshTransport.push` / `.pull` via subprocess `rsync`

**Files:**
- Modify: `src/ray_dispatcher/ssh.py` (append `SshTransport` with push/pull; `run` lands in Task 5)
- Test: `tests/unit/test_ssh_transport.py`

**Interfaces:**
- Consumes: `SshConfig`, `build_rsync_argv`, `TransportError`.
- Produces: `SshTransport(cfg: SshConfig)` whose `push`/`pull` form the `user@host:` remote side, call `build_rsync_argv`, and run `subprocess.run(argv, check=True, capture_output=True, text=True)`, wrapping failures in `TransportError`. The Fabric connection is created lazily (Task 5), so push/pull need no network.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_ssh_transport.py`:

```python
import subprocess

import pytest

from ray_dispatcher import ssh
from ray_dispatcher.ssh import SshConfig, SshTransport, TransportError


def _cfg():
    return SshConfig(host="h", user="u", port=22, identity_file=None,
                     known_hosts_file="/kh")


def test_push_builds_remote_dst_and_runs_rsync(monkeypatch):
    seen = {}

    def fake_run(argv, **kw):
        seen["argv"] = argv
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(ssh.subprocess, "run", fake_run)
    SshTransport(_cfg()).push("/local/dir", "/remote/dir", delete=True, excludes=(".git/",))
    assert seen["argv"][0] == "rsync"
    assert seen["argv"][-2:] == ["/local/dir", "u@h:/remote/dir"]
    assert "--delete" in seen["argv"]


def test_pull_builds_remote_src(monkeypatch):
    seen = {}
    monkeypatch.setattr(ssh.subprocess, "run",
                        lambda argv, **kw: seen.update(argv=argv) or
                        subprocess.CompletedProcess(argv, 0, "", ""))
    SshTransport(_cfg()).pull("/remote/out", "/local/out")
    assert seen["argv"][-2:] == ["u@h:/remote/out", "/local/out"]


def test_rsync_failure_raises_transport_error(monkeypatch):
    def boom(argv, **kw):
        raise subprocess.CalledProcessError(23, argv, stderr="rsync: link failed")

    monkeypatch.setattr(ssh.subprocess, "run", boom)
    with pytest.raises(TransportError):
        SshTransport(_cfg()).push("/a", "/b")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_ssh_transport.py -v`
Expected: FAIL with `ImportError: cannot import name 'SshTransport'`.

- [ ] **Step 3: Write the implementation (append to `ssh.py`)**

Add to the imports block of `ssh.py`:

```python
import subprocess
from typing import Any
```

Append to `ssh.py`:

```python
class SshTransport:
    """Fabric for run (Task 5), OpenSSH rsync for push/pull. The Fabric
    connection is created lazily so file transfer needs no live connection."""

    def __init__(self, cfg: SshConfig) -> None:
        self.cfg = cfg
        self._conn: Any = None  # lazily-built fabric.Connection (Task 5)

    def push(
        self, local: str, remote: str, *, delete: bool = False, excludes: Sequence[str] = ()
    ) -> None:
        dst = f"{self.cfg.user}@{self.cfg.host}:{remote}"
        self._rsync(build_rsync_argv(self.cfg, local, dst, delete=delete, excludes=excludes))

    def pull(
        self, remote: str, local: str, *, delete: bool = False, excludes: Sequence[str] = ()
    ) -> None:
        src = f"{self.cfg.user}@{self.cfg.host}:{remote}"
        self._rsync(build_rsync_argv(self.cfg, src, local, delete=delete, excludes=excludes))

    def _rsync(self, argv: list[str]) -> None:
        try:
            subprocess.run(argv, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as exc:
            raise TransportError(f"rsync failed ({exc.returncode}): {exc.stderr}") from exc
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_ssh_transport.py -v`
Expected: all cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/ssh.py tests/unit/test_ssh_transport.py
git commit -m "feat: add SshTransport push/pull over rsync"
```

---

### Task 5: `build_connection` + `SshTransport.run` via Fabric

**Files:**
- Modify: `src/ray_dispatcher/ssh.py` (append `build_connection`; add `run` to `SshTransport`)
- Modify: `pyproject.toml` (mypy override for untyped SSH libraries)
- Test: `tests/unit/test_ssh_connection.py`

**Interfaces:**
- Consumes: `SshConfig`, `CommandResult`, `TransportError`, `fabric`, `paramiko`.
- Produces: `build_connection(cfg: SshConfig) -> fabric.Connection` with host/user/port, `key_filename` in `connect_kwargs`, host keys loaded from `cfg.known_hosts_file`, and a `RejectPolicy` for unknown hosts. `SshTransport.run(argv, *, timeout_s=None) -> CommandResult` shells the controlled argv with `shlex.join`, runs via Fabric (`hide=True, warn=True`), and returns a `CommandResult`; failures wrap in `TransportError`. The live network behavior of `run` is covered by the Phase 7 multipass e2e tests.

- [ ] **Step 1: Add the mypy override (untyped third-party SSH libs)**

Append to `pyproject.toml`:

```toml
[[tool.mypy.overrides]]
module = ["fabric.*", "invoke.*", "paramiko.*"]
ignore_missing_imports = true
```

(Fabric/invoke/paramiko ship no type stubs; without this, mypy `strict` errors on the import.)

- [ ] **Step 2: Write the failing test**

`tests/unit/test_ssh_connection.py`:

```python
import paramiko

from ray_dispatcher.ssh import SshConfig, build_connection


def test_build_connection_sets_host_user_port_and_identity(tmp_path):
    kh = tmp_path / "known_hosts"
    kh.write_text("")
    cfg = SshConfig(host="10.0.0.5", user="ubuntu", port=2222,
                    identity_file="/keys/id", known_hosts_file=str(kh))
    conn = build_connection(cfg)
    assert conn.host == "10.0.0.5"
    assert conn.user == "ubuntu"
    assert conn.port == 2222
    assert conn.connect_kwargs["key_filename"] == "/keys/id"
    # host-key checking is enforced via a RejectPolicy on the paramiko client
    assert isinstance(conn.client._policy, paramiko.RejectPolicy)


def test_build_connection_without_identity_uses_agent(tmp_path):
    kh = tmp_path / "known_hosts"
    kh.write_text("")
    cfg = SshConfig(host="h", user="u", port=22, identity_file=None,
                    known_hosts_file=str(kh))
    conn = build_connection(cfg)
    assert "key_filename" not in conn.connect_kwargs
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_ssh_connection.py -v`
Expected: FAIL with `ImportError: cannot import name 'build_connection'`.

- [ ] **Step 4: Write the implementation (append to `ssh.py`)**

Add to the imports block of `ssh.py`:

```python
import time

import fabric
import paramiko
```

Append `build_connection` to `ssh.py`:

```python
def build_connection(cfg: SshConfig) -> fabric.Connection:
    """A Fabric connection that enforces host-key checking against cfg.known_hosts_file."""
    connect_kwargs: dict[str, object] = {}
    if cfg.identity_file:
        connect_kwargs["key_filename"] = cfg.identity_file
    conn = fabric.Connection(
        host=cfg.host, user=cfg.user, port=cfg.port, connect_kwargs=connect_kwargs
    )
    client = conn.client  # lazily-created paramiko.SSHClient
    client.load_host_keys(cfg.known_hosts_file)
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    return conn
```

Add a `run` method to the `SshTransport` class (place it above `_rsync`):

```python
    def run(self, argv: Sequence[str], *, timeout_s: float | None = None) -> CommandResult:
        if self._conn is None:
            self._conn = build_connection(self.cfg)
        command = shlex.join(argv)  # argv is library-controlled; never a user job string
        start = time.monotonic()
        try:
            result = self._conn.run(
                command, hide=True, warn=True, timeout=timeout_s, in_stream=False
            )
        except Exception as exc:  # noqa: BLE001 — uniform seam failure
            raise TransportError(f"ssh run failed: {exc}") from exc
        return CommandResult(
            returncode=result.exited,
            stdout=result.stdout,
            stderr=result.stderr,
            duration_s=time.monotonic() - start,
        )
```

(`self._conn` is typed `Any` (Task 4), so the lazy Fabric connection needs no
cast or `type: ignore`. `fabric`/`paramiko` are untyped, covered by the mypy
override added in Step 1.)

- [ ] **Step 5: Run test + type check to verify they pass**

Run: `uv run pytest tests/unit/test_ssh_connection.py -v`
Expected: both cases `passed`.

Run: `uv run mypy`
Expected: `Success: no issues found`.

- [ ] **Step 6: Commit**

```bash
git add src/ray_dispatcher/ssh.py pyproject.toml tests/unit/test_ssh_connection.py
git commit -m "feat: add Fabric connection builder and SshTransport.run"
```

---

### Task 6: `remote_runner.py` — manifest, env, Popen, capture, result

The standalone VM-side supervisor. Tested locally by invoking it as a subprocess against a temp manifest.

**Files:**
- Create: `src/ray_dispatcher/remote_runner.py`
- Test: `tests/unit/test_remote_runner.py`

**Interfaces:**
- Consumes: nothing from `ray_dispatcher` (stdlib only).
- Produces: a script runnable as `python remote_runner.py <manifest.json>`. Manifest keys: `argv: list[str]`, `cwd: str`, `env: dict`, `secret_env: dict`, `venv_bin: str`, `virtual_env: str`, `stdout_path`, `stderr_path`, `pid_path`, `result_path`. Functions: `build_env(manifest) -> dict[str, str]`, `run(manifest) -> int`, `main(argv) -> int`. Writes `pid_path` = `{"pid", "pgid"}`, `result_path` = `{"returncode", "started_at", "ended_at", "duration_s"}`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_remote_runner.py`:

```python
import json
import subprocess
import sys
from pathlib import Path

RUNNER = str(Path("src/ray_dispatcher/remote_runner.py").resolve())


def _manifest(tmp_path, argv, env=None, secret_env=None):
    m = {
        "argv": argv,
        "cwd": str(tmp_path),
        "env": env or {},
        "secret_env": secret_env or {},
        "venv_bin": str(tmp_path / "venv" / "bin"),
        "virtual_env": str(tmp_path / "venv"),
        "stdout_path": str(tmp_path / "out.log"),
        "stderr_path": str(tmp_path / "err.log"),
        "pid_path": str(tmp_path / "pid.json"),
        "result_path": str(tmp_path / "result.json"),
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(m))
    return m, str(path)


def _invoke(manifest_path):
    return subprocess.run([sys.executable, RUNNER, manifest_path],
                          capture_output=True, text=True)


def test_runner_captures_output_and_returncode(tmp_path):
    m, path = _manifest(
        tmp_path,
        [sys.executable, "-c", "import sys; sys.stdout.write('hi'); "
                              "sys.stderr.write('warn'); sys.exit(3)"],
    )
    proc = _invoke(path)
    assert proc.returncode == 0  # runner managed the child successfully
    assert Path(m["stdout_path"]).read_bytes() == b"hi"
    assert Path(m["stderr_path"]).read_bytes() == b"warn"
    result = json.loads(Path(m["result_path"]).read_text())
    assert result["returncode"] == 3
    assert result["duration_s"] >= 0
    pid = json.loads(Path(m["pid_path"]).read_text())
    assert isinstance(pid["pid"], int) and isinstance(pid["pgid"], int)


def test_runner_applies_venv_and_secret_env(tmp_path):
    m, path = _manifest(
        tmp_path,
        [sys.executable, "-c",
         "import os; open(os.environ['OUT'],'w').write("
         "os.environ['PATH'].split(os.pathsep)[0] + '|' + "
         "os.environ['VIRTUAL_ENV'] + '|' + os.environ['LIC'])"],
        env={"OUT": str(tmp_path / "probe.txt")},
        secret_env={"LIC": "/remote/gurobi.lic"},
    )
    _invoke(path)
    first_path, venv, lic = (tmp_path / "probe.txt").read_text().split("|")
    assert first_path == m["venv_bin"]
    assert venv == m["virtual_env"]
    assert lic == "/remote/gurobi.lic"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_remote_runner.py -v`
Expected: FAIL (file `src/ray_dispatcher/remote_runner.py` does not exist → runner invocation errors / FileNotFoundError).

- [ ] **Step 3: Write the implementation**

`src/ray_dispatcher/remote_runner.py`:

```python
#!/usr/bin/env python3
"""Standalone remote subprocess supervisor — runs ON the VM (spec §7).

Invoked as: ``python remote_runner.py <manifest.json>``. Stdlib only; it must
NOT import from ray_dispatcher, because it runs in the project venv where the
package is absent. The job argv runs via Popen (no shell).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from typing import IO, Any


def build_env(manifest: dict[str, Any]) -> dict[str, str]:
    env = os.environ.copy()
    env["PATH"] = manifest["venv_bin"] + os.pathsep + env.get("PATH", "")
    env["VIRTUAL_ENV"] = manifest["virtual_env"]
    env.update(manifest.get("env", {}))
    env.update(manifest.get("secret_env", {}))
    return env


def _drain(stream: IO[bytes], raw_path: str) -> None:
    with open(raw_path, "wb") as raw:
        for chunk in iter(lambda: stream.read(4096), b""):
            raw.write(chunk)
            raw.flush()


def run(manifest: dict[str, Any]) -> int:
    env = build_env(manifest)
    started = time.time()
    proc = subprocess.Popen(
        manifest["argv"],
        cwd=manifest["cwd"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    assert proc.stdout is not None and proc.stderr is not None
    with open(manifest["pid_path"], "w") as fh:
        json.dump({"pid": proc.pid, "pgid": os.getpgid(proc.pid)}, fh)
    threads = [
        threading.Thread(target=_drain, args=(proc.stdout, manifest["stdout_path"])),
        threading.Thread(target=_drain, args=(proc.stderr, manifest["stderr_path"])),
    ]
    for t in threads:
        t.start()
    proc.wait()
    for t in threads:
        t.join()
    ended = time.time()
    with open(manifest["result_path"], "w") as fh:
        json.dump(
            {
                "returncode": proc.returncode,
                "started_at": started,
                "ended_at": ended,
                "duration_s": ended - started,
            },
            fh,
        )
    return proc.returncode


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: remote_runner.py <manifest.json>", file=sys.stderr)
        return 2
    with open(argv[1]) as fh:
        manifest = json.load(fh)
    run(manifest)
    return 0  # the runner managed the child; the child's rc is in result.json


if __name__ == "__main__":
    sys.exit(main(sys.argv))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_remote_runner.py -v`
Expected: both cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/remote_runner.py tests/unit/test_remote_runner.py
git commit -m "feat: add standalone remote_runner (manifest, popen, capture, result)"
```

---

### Task 7: `remote_runner.py` — live forwarding + binary-safe markers

Adds live forwarding (so the host's SSH channel carries output as it happens) on top of the remote file copies (§7 step 6). The runner forwards the bytes **unchanged**; the host applies replacement markers when it writes its streamed log copy (§7 step 7, Phase 5), while the VM-side file keeps the exact bytes.

**Files:**
- Modify: `src/ray_dispatcher/remote_runner.py` (replace `_drain` with a tee; wire forward streams)
- Test: `tests/unit/test_remote_runner_forward.py`

**Interfaces:**
- Consumes: Task 6's runner.
- Produces: child stdout/stderr are both written raw to the remote files **and** forwarded byte-for-byte to the runner's own `sys.stdout.buffer` / `sys.stderr.buffer`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_remote_runner_forward.py`:

```python
import json
import subprocess
import sys
from pathlib import Path

RUNNER = str(Path("src/ray_dispatcher/remote_runner.py").resolve())


def _manifest(tmp_path, argv):
    m = {
        "argv": argv, "cwd": str(tmp_path), "env": {}, "secret_env": {},
        "venv_bin": str(tmp_path / "v" / "bin"), "virtual_env": str(tmp_path / "v"),
        "stdout_path": str(tmp_path / "out.log"), "stderr_path": str(tmp_path / "err.log"),
        "pid_path": str(tmp_path / "pid.json"), "result_path": str(tmp_path / "result.json"),
    }
    p = tmp_path / "m.json"
    p.write_text(json.dumps(m))
    return m, str(p)


def test_child_output_is_forwarded_live_to_runner_streams(tmp_path):
    m, path = _manifest(tmp_path, [sys.executable, "-c",
                                   "import sys; print('forward-me'); "
                                   "print('to-stderr', file=sys.stderr)"])
    proc = subprocess.run([sys.executable, RUNNER, path], capture_output=True, text=True)
    assert "forward-me" in proc.stdout
    assert "to-stderr" in proc.stderr


def test_binary_output_preserved_in_file_and_forwarded_raw(tmp_path):
    m, path = _manifest(tmp_path, [sys.executable, "-c",
                                   "import sys; sys.stdout.buffer.write(b'\\xff\\xfe')"])
    proc = subprocess.run([sys.executable, RUNNER, path], capture_output=True)
    # raw bytes preserved on the VM-side file AND forwarded byte-for-byte
    assert Path(m["stdout_path"]).read_bytes() == b"\xff\xfe"
    assert b"\xff\xfe" in proc.stdout
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_remote_runner_forward.py -v`
Expected: FAIL (`forward-me` not in the runner's stdout — Task 6 only wrote to files).

- [ ] **Step 3: Update the implementation**

In `src/ray_dispatcher/remote_runner.py`, replace `_drain` with `_tee` (the
`from typing import IO, Any` line from Task 6 is unchanged):

```python
def _tee(stream: IO[bytes], raw_path: str, forward: IO[bytes]) -> None:
    """Write child output raw to ``raw_path`` and forward the same bytes to
    ``forward`` (a binary stream) so the host SSH channel carries it live. The
    VM-side file keeps the exact bytes; the host adds replacement markers when
    it writes its streamed copy (Phase 5)."""
    with open(raw_path, "wb") as raw:
        for chunk in iter(lambda: stream.read(4096), b""):
            raw.write(chunk)
            raw.flush()
            forward.write(chunk)
            forward.flush()
```

In `run(...)`, replace the two thread definitions with (note `.buffer` — the
binary side of the runner's own stdout/stderr):

```python
    threads = [
        threading.Thread(
            target=_tee, args=(proc.stdout, manifest["stdout_path"], sys.stdout.buffer)
        ),
        threading.Thread(
            target=_tee, args=(proc.stderr, manifest["stderr_path"], sys.stderr.buffer)
        ),
    ]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_remote_runner_forward.py tests/unit/test_remote_runner.py -v`
Expected: all cases `passed` (Task 6 tests still green — files still receive raw bytes).

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/remote_runner.py tests/unit/test_remote_runner_forward.py
git commit -m "feat: forward remote_runner child output live with binary-safe markers"
```

---

### Task 8: Host-side process-group termination + Phase 2 gate

**Files:**
- Modify: `src/ray_dispatcher/ssh.py` (append `terminate_process_group`)
- Test: `tests/unit/test_terminate_pgroup.py`

**Interfaces:**
- Consumes: `Transport`, `CommandResult` (the terminator signals via `transport.run`, so it works over any Transport and is unit-testable with `FakeTransport`).
- Produces: `terminate_process_group(transport, pgid, *, grace_s=10.0, poll_s=0.5, now=time.monotonic, sleep=time.sleep) -> bool` — sends `kill -TERM -<pgid>`, polls `kill -0 -<pgid>` until the group is gone or grace expires, then `kill -KILL -<pgid>`; returns `True` once the group is gone. `now`/`sleep` are injectable for deterministic tests.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_terminate_pgroup.py`:

```python
from ray_dispatcher.ssh import CommandResult, FakeTransport, terminate_process_group


def _alive_for(n_checks):
    """run_results: 'kill -0' returns alive (rc 0) for the first n checks, then dead (rc 1)."""
    state = {"checks": 0}

    def results(argv):
        if argv[:2] == ["kill", "-0"]:
            state["checks"] += 1
            return CommandResult(0 if state["checks"] <= n_checks else 1, "", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    return results


def test_term_is_enough_when_process_exits():
    t = FakeTransport(run_results=_alive_for(0))  # dead on first probe
    assert terminate_process_group(t, 4321, sleep=lambda s: None) is True
    sent = [c[1] for c in t.calls if c[0] == "run"]
    assert ("kill", "-TERM", "-4321") in sent
    assert ("kill", "-KILL", "-4321") not in sent  # never needed to escalate


def test_escalates_to_kill_after_grace():
    # stays alive through the grace window, then dies after KILL
    t = FakeTransport(run_results=_alive_for(100))
    fake_clock = {"t": 0.0}

    def now():
        return fake_clock["t"]

    def sleep(s):
        fake_clock["t"] += s

    # process is still "alive" on probes during grace; flip to dead after KILL:
    calls = {"kill_sent": False}

    def results(argv):
        if argv == ["kill", "-KILL", "-9"]:
            calls["kill_sent"] = True
            return CommandResult(0, "", "", 0.0)
        if argv[:2] == ["kill", "-0"]:
            return CommandResult(1 if calls["kill_sent"] else 0, "", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    t = FakeTransport(run_results=results)
    assert terminate_process_group(t, 9, grace_s=1.0, poll_s=0.5, now=now, sleep=sleep) is True
    sent = [c[1] for c in t.calls if c[0] == "run"]
    assert ("kill", "-KILL", "-9") in sent
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_terminate_pgroup.py -v`
Expected: FAIL with `ImportError: cannot import name 'terminate_process_group'`.

- [ ] **Step 3: Write the implementation (append to `ssh.py`)**

Append to `ssh.py`:

```python
def terminate_process_group(
    transport: Transport,
    pgid: int,
    *,
    grace_s: float = 10.0,
    poll_s: float = 0.5,
    now: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    """SIGTERM the remote process group, wait up to grace_s, then SIGKILL.
    Returns True once a probe confirms the group is gone (spec §8.1)."""

    def gone() -> bool:
        return transport.run(["kill", "-0", f"-{pgid}"]).returncode != 0

    transport.run(["kill", "-TERM", f"-{pgid}"])
    deadline = now() + grace_s
    while now() < deadline:
        if gone():
            return True
        sleep(poll_s)
    transport.run(["kill", "-KILL", f"-{pgid}"])
    sleep(poll_s)
    return gone()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_terminate_pgroup.py -v`
Expected: both cases `passed`.

- [ ] **Step 5: Run the full Phase 2 gate**

Run: `uv run pytest -q`
Expected: all tests `passed` (Phase 1 + Phase 2).

Run: `uv run ruff check --fix . && uv run ruff check . && uv run mypy`
Expected: ruff auto-sorts then reports `All checks passed!`; mypy reports `Success: no issues found`.

- [ ] **Step 6: Commit**

```bash
git add src/ray_dispatcher/ssh.py tests/unit/test_terminate_pgroup.py
git commit -m "feat: add host-side process-group termination (SIGTERM/SIGKILL with probe)"
```

---

## Phase 2 self-review

Run before declaring Phase 2 done:

- [ ] `SshConfig.from_host` rejects missing identity/known-hosts files (closes the §4.1 deferral); rsync and Fabric both derive from the same `SshConfig`.
- [ ] Only `ssh.py` builds Fabric connections / rsync invocations; only `remote_runner.py` spawns the application subprocess.
- [ ] `remote_runner.py` imports nothing from `ray_dispatcher` (grep it); uses `start_new_session=True`; records PID+PGID; tees to remote files **and** forwards the same bytes live; the VM-side file keeps exact bytes (host applies replacement markers in Phase 5).
- [ ] `terminate_process_group` sends SIGTERM, escalates to SIGKILL after grace, and confirms via a probe (`kill -0`).
- [ ] Host-key checking is enforced in both transports (rsync `StrictHostKeyChecking=yes` + `UserKnownHostsFile`; Fabric `RejectPolicy` + `load_host_keys`).
- [ ] `uv run pytest -q`, `uv run ruff check .`, and `uv run mypy` are all green.

**Deliverable:** host-side transport (`SshConfig`, `Transport`/`SshTransport`/`FakeTransport`, rsync push/pull, Fabric run, process-group terminator) and the standalone VM-side `remote_runner.py`, fully unit-tested with fakes and a local subprocess. Live SSH behavior of `SshTransport.run`/`push`/`pull` against real VMs is exercised by the Phase 7 multipass e2e suite.
