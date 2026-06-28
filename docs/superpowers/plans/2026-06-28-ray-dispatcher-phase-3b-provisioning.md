# ray-dispatcher — Phase 3b: Provisioning — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `provisioning.py` — the per-host provisioning algorithm (spec §6.3) plus inventory-level orchestration that produces a `ProvisioningReport` and holds the session locks for healthy hosts.

**Architecture:** A `HostProvisioner` drives one host through the §6.3 steps, each step a sequence of library-controlled commands run through the Phase 2 `Transport` seam. The remote `$HOME` is resolved once per host, then every remote path is absolute (so both `run` and rsync `push` are unambiguous). Content digests (Phase 3a `digests.py`) gate cache reuse; the remote session lock + heartbeat (Phase 3a `locking.py`) is acquired before provisioning and kept live for healthy hosts. An inventory-level `provision()` runs hosts in a bounded thread pool, applies `require_all_hosts`/`force`, and returns a `ProvisioningOutcome` (the public `ProvisioningReport` plus the live locks to release at teardown).

**Tech Stack:** Python ≥3.10 stdlib (`json`, `shlex`, `secrets`, `concurrent.futures`, `dataclasses`), Phase 2 `ssh` (`Transport`/`SshConfig`/`SshTransport`), Phase 3a `digests`+`locking`, pytest/ruff/mypy.

**Spec:** `docs/superpowers/specs/2026-06-27-ray-dispatcher-design.md` — §6.1 remote layout, §6.2 digests, §6.3 provisioning algorithm, §3.2.6 session lock, §7 no-shell-from-user-strings. Phases 1, 2, 3a are on `main`.

## Global Constraints

Every task implicitly includes these. Values copied verbatim from the spec.

- `requires-python = ">=3.10"`; mypy runs `strict` over `src` only; ruff `select = E,F,I,UP,B`, line-length 100.
- **Remote layout (§6.1)**, under `~/.ray_dispatcher/` (resolved to an absolute path via the SSH user's `$HOME`):
  - `bin/<runner_digest>/remote_runner.py`
  - `projects/<project_id>/source/` and `projects/<project_id>/source-manifest.json`
  - `projects/<project_id>/envs/<environment_digest>/{.venv/, environment-manifest.json}`
  - `secrets/<project_id>/...`
  - `project_id` is stable — it is NOT a content hash.
- **Provisioning algorithm, per host, parallel but bounded by inventory size (§6.3):**
  1. Verify SSH host key + auth, then atomically acquire the remote session lock (heartbeat-backed). A live lock from another session raises `HostInUseError`. A host-side heartbeat keeps the lock live throughout provisioning and execution. (Full stale-lock reconciliation — "no live runner owned by that session" — is Phase 4; Phase 3a/3b use the TTL-based takeover already built.)
  2. Verify required disk space, `python3`, and `rsync`.
  3. Install the exact uv version from its versioned official installer URL into a dispatcher-owned, version-specific path. Never replace or depend on a system-wide `uv`. Verify the installed binary's version before use.
  4. Run `uv python install <exact-version>` and verify the resolved interpreter version.
  5. Rsync source into a staging directory with `--delete` and configured excludes, then atomically replace `source/`. Write `source-manifest.json` only after the sync succeeds.
  6. If the environment manifest for `environment_digest` is absent or invalid, create it in staging, set `UV_PROJECT_ENVIRONMENT` to the staging `.venv`, and run `uv sync --project <source> --locked --no-install-project --no-install-workspace --no-default-groups --python <exact-version>` (explicit dependency groups add `--group` args). Atomically publish the environment only after sync and interpreter smoke checks succeed. Published environments are logically immutable.
  7. Install the versioned remote runner.
  8. Copy secrets with their declared modes and verify ownership without printing secret contents.
- `force=True` repeats validation and transfer but does not weaken atomicity (re-runs skip-if-present checks). It applies only before the Ray runtime has started.
- `require_all_hosts=True` makes any failure abort setup after collecting a complete `ProvisioningReport`, and releases every lock acquired. When false, failed hosts are marked unavailable; execution may proceed only if ≥1 host is healthy, else `NoHealthyHostsError`.
- **Secrets are excluded from all digests (§6.2)** and their contents are never printed (§6.3.8).
- **No shell from user strings (§7):** every remote command embeds only library-controlled values (resolved paths, exact versions, a generated session id) and `shlex.quote`s all interpolated data. No `Job.command`/user string is involved in provisioning.

## Existing interfaces this plan composes (already on `main`)

- `models.py`: `RemoteHost(host,user,slots=1,port=22,identity_file=None,known_hosts_file=...)`, `Inventory(hosts)`, `Project(path,project_id,python,uv_version,secrets=(),exclude=(".venv/",".git/","solutions/"),dependency_groups=())`, `SecretFile(source,remote_name,env_var=None,mode=0o600)`, `HostProvisioningResult(host,succeeded,source_digest,environment_digest,error=None)`, `ProvisioningReport(hosts)`.
- `ssh.py`: `Transport` Protocol — `run(argv,*,timeout_s=None)->CommandResult(returncode,stdout,stderr,duration_s)`, `push(local,remote,*,delete=False,excludes=())`, `pull(...)`; `SshConfig.from_host(RemoteHost)`, `SshTransport(cfg)`, `TransportError`; `FakeTransport(run_results=callback)` where the callback receives `list(argv)` and `.calls` records `("run",tuple(argv))`/`("push",(local,remote,delete,excludes))`.
- `digests.py`: `source_digest(root,excludes)->str`, `environment_digest(project,*,platform,sync_flags)->str`, `runner_digest(runner_path)->str`.
- `locking.py`: `SessionLock(transport,session_id,*,ttl_s=60.0,now=time.time)` with `acquire()`/`heartbeat()`/`release()`; `HeartbeatThread(lock,interval_s)` with `start()`/`stop()`.
- `errors.py`: `ProvisioningError(report,message=None)` (`.report`), `HostInUseError`, `NoHealthyHostsError`, `TransportError`.

## Full-project file structure (only Phase 3b file is built here)

```text
src/ray_dispatcher/
├── errors.py / paths.py / models.py        # Phase 1
├── ssh.py / remote_runner.py               # Phase 2
├── digests.py / locking.py                 # Phase 3a
├── provisioning.py   # THIS PLAN — RemoteLayout, HostProvisioner, provision(), ProvisioningOutcome
├── scheduling.py     # Phase 4
├── results.py        # Phase 5
├── backends/…        # Phase 5-6
└── dispatcher.py     # Phase 6
```

### Phase 3b file structure

- Create: `src/ray_dispatcher/provisioning.py`
- Test: `tests/unit/test_provisioning_layout.py`, `test_provisioning_preflight.py`, `test_provisioning_uv.py`, `test_provisioning_python.py`, `test_provisioning_source.py`, `test_provisioning_env.py`, `test_provisioning_runner.py`, `test_provisioning_secrets.py`, `test_provisioning_host.py`, `test_provisioning_inventory.py`

### Shared test helpers (define inline per test file, copy as needed)

```python
def _runs(t):           # list of argv-tuples for run() calls
    return [c[1] for c in t.calls if c[0] == "run"]

def _scripts(t):        # the sh -c script strings
    return [a[2] for a in _runs(t) if len(a) >= 3 and a[0] == "sh" and a[1] == "-c"]

def _pushes(t):         # list of (local, remote, delete, excludes)
    return [c[1] for c in t.calls if c[0] == "push"]
```

---

### Task 1: `RemoteLayout` + `HostProvisioner` skeleton + remote-command helpers

**Files:**
- Create: `src/ray_dispatcher/provisioning.py`
- Test: `tests/unit/test_provisioning_layout.py`

**Interfaces:**
- Produces:
  - `RemoteLayout(home: str, project_id: str)` with attributes `root`, `project`, `source`, `source_manifest`, `secrets`, `uv_root` and methods `env_dir(d)`, `env_venv(d)`, `env_manifest(d)`, `runner_dir(d)`, `runner(d)`, `uv_bin(version)` — all absolute POSIX paths under `<home>/.ray_dispatcher`.
  - internal `_StepError(Exception)`.
  - `HostProvisioner(transport, project, host, *, runner_path, session_id, force=False, min_disk_mb=500, heartbeat_interval_s=20.0)` with helpers `_checked(argv, what, *, timeout_s=None) -> CommandResult` (raises `_StepError` on non-zero), `_write_remote_file(path, content, *, mode=None)`, and `_resolve_layout() -> RemoteLayout`. `self.layout` is set by the driver later; until then it is `None`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_provisioning_layout.py`:

```python
import pytest

from ray_dispatcher.models import Project, RemoteHost
from ray_dispatcher.provisioning import HostProvisioner, RemoteLayout, _StepError
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _runs(t):
    return [c[1] for c in t.calls if c[0] == "run"]


def _project():
    return Project(path="/local/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25")


def _host():
    return RemoteHost(host="10.0.0.1", user="ubuntu")


def test_layout_paths_are_absolute_under_home():
    lo = RemoteLayout("/home/ubuntu", "dfaas")
    assert lo.root == "/home/ubuntu/.ray_dispatcher"
    assert lo.source == "/home/ubuntu/.ray_dispatcher/projects/dfaas/source"
    assert lo.source_manifest == "/home/ubuntu/.ray_dispatcher/projects/dfaas/source-manifest.json"
    assert lo.secrets == "/home/ubuntu/.ray_dispatcher/secrets/dfaas"
    assert lo.env_dir("abc") == "/home/ubuntu/.ray_dispatcher/projects/dfaas/envs/abc"
    assert lo.env_venv("abc").endswith("/envs/abc/.venv")
    assert lo.env_manifest("abc").endswith("/envs/abc/environment-manifest.json")
    assert lo.runner("deadbeef").endswith("/bin/deadbeef/remote_runner.py")
    assert lo.uv_bin("0.11.25").endswith("/uv/0.11.25/uv")


def test_resolve_layout_probes_home():
    def results(argv):
        if argv[:2] == ["sh", "-c"] and "$HOME" in argv[2]:
            return CommandResult(0, "/home/ubuntu\n", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    t = FakeTransport(run_results=results)
    p = HostProvisioner(t, _project(), _host(), runner_path="x", session_id="s")
    lo = p._resolve_layout()
    assert lo.source == "/home/ubuntu/.ray_dispatcher/projects/dfaas/source"


def test_checked_raises_step_error_on_nonzero():
    def results(argv):
        return CommandResult(3, "", "boom", 0.0)

    t = FakeTransport(run_results=results)
    p = HostProvisioner(t, _project(), _host(), runner_path="x", session_id="s")
    with pytest.raises(_StepError, match="boom"):
        p._checked(["false"], "do thing")


def test_write_remote_file_is_atomic_tmp_then_mv():
    t = FakeTransport()  # default rc 0
    p = HostProvisioner(t, _project(), _host(), runner_path="x", session_id="s")
    p._write_remote_file("/home/ubuntu/.ray_dispatcher/projects/dfaas/source-manifest.json", '{"a":1}')
    script = _runs(t)[-1][2]
    assert "printf %s" in script and "source-manifest.json.tmp" in script and "mv -f" in script
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_provisioning_layout.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ray_dispatcher.provisioning'`.

- [ ] **Step 3: Write the implementation**

`src/ray_dispatcher/provisioning.py`:

```python
"""Per-host provisioning (spec §6.3) and inventory orchestration.

Drives each host through the §6.3 steps over the Phase 2 Transport seam. The
remote $HOME is resolved once per host (one `printf %s "$HOME"` probe), after
which every remote path is absolute — so both `run` (via shlex.join) and rsync
`push` (which uses --protect-args and would NOT expand `~`) are unambiguous.
All interpolated data is shlex-quoted; no user string is ever shelled (§7).
"""

from __future__ import annotations

import shlex

from .models import Project, RemoteHost
from .ssh import CommandResult, Transport


class _StepError(Exception):
    """A provisioning step failed on one host. Caught by the host driver and
    turned into a failed HostProvisioningResult; never escapes provisioning.py."""


class RemoteLayout:
    """Absolute remote paths under <home>/.ray_dispatcher (spec §6.1)."""

    def __init__(self, home: str, project_id: str) -> None:
        home = home.rstrip("/")
        self.root = f"{home}/.ray_dispatcher"
        self.project = f"{self.root}/projects/{project_id}"
        self.source = f"{self.project}/source"
        self.source_manifest = f"{self.project}/source-manifest.json"
        self.secrets = f"{self.root}/secrets/{project_id}"
        self.uv_root = f"{self.root}/uv"

    def env_dir(self, environment_digest: str) -> str:
        return f"{self.project}/envs/{environment_digest}"

    def env_venv(self, environment_digest: str) -> str:
        return f"{self.env_dir(environment_digest)}/.venv"

    def env_manifest(self, environment_digest: str) -> str:
        return f"{self.env_dir(environment_digest)}/environment-manifest.json"

    def runner_dir(self, runner_digest: str) -> str:
        return f"{self.root}/bin/{runner_digest}"

    def runner(self, runner_digest: str) -> str:
        return f"{self.runner_dir(runner_digest)}/remote_runner.py"

    def uv_bin(self, uv_version: str) -> str:
        return f"{self.uv_root}/{uv_version}/uv"


class HostProvisioner:
    """Provisions one host. `layout` is None until the driver resolves $HOME."""

    def __init__(
        self,
        transport: Transport,
        project: Project,
        host: RemoteHost,
        *,
        runner_path: str,
        session_id: str,
        force: bool = False,
        min_disk_mb: int = 500,
        heartbeat_interval_s: float = 20.0,
    ) -> None:
        self.t = transport
        self.project = project
        self.host = host
        self.runner_path = runner_path
        self.session_id = session_id
        self.force = force
        self.min_disk_mb = min_disk_mb
        self.heartbeat_interval_s = heartbeat_interval_s
        self.layout: RemoteLayout | None = None

    # --- helpers -------------------------------------------------------------

    def _checked(
        self, argv: list[str], what: str, *, timeout_s: float | None = None
    ) -> CommandResult:
        r = self.t.run(argv, timeout_s=timeout_s)
        if r.returncode != 0:
            detail = (r.stderr or r.stdout).strip()
            raise _StepError(f"{what} failed on {self.host.host} (rc={r.returncode}): {detail}")
        return r

    def _write_remote_file(self, path: str, content: str, *, mode: int | None = None) -> None:
        tmp = path + ".tmp"
        qtmp, qpath = shlex.quote(tmp), shlex.quote(path)
        chmod = f" && chmod {mode:o} {qtmp}" if mode is not None else ""
        self._checked(
            ["sh", "-c", f"printf %s {shlex.quote(content)} > {qtmp}{chmod} && mv -f {qtmp} {qpath}"],
            f"write {path}",
        )

    def _resolve_layout(self) -> RemoteLayout:
        home = self._checked(["sh", "-c", 'printf %s "$HOME"'], "resolve $HOME").stdout.strip()
        if not home:
            raise _StepError(f"could not resolve remote $HOME on {self.host.host}")
        return RemoteLayout(home, self.project.project_id)

    @property
    def _lo(self) -> RemoteLayout:
        if self.layout is None:  # pragma: no cover - guarded by driver ordering
            raise _StepError("layout not resolved")
        return self.layout
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_provisioning_layout.py -v`
Expected: all cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/provisioning.py tests/unit/test_provisioning_layout.py
git commit -m "feat: add RemoteLayout + HostProvisioner skeleton + remote-command helpers"
```

---

### Task 2: `_preflight` — verify python3, rsync, disk (§6.3.2)

**Files:**
- Modify: `src/ray_dispatcher/provisioning.py` (append method)
- Test: `tests/unit/test_provisioning_preflight.py`

**Interfaces:**
- Consumes: `HostProvisioner._checked`, `self._lo` (Task 1).
- Produces: `HostProvisioner._preflight() -> None` — verifies `command -v python3`, `command -v rsync`, and ≥ `min_disk_mb` available under the dispatcher root; raises `_StepError` otherwise.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_provisioning_preflight.py`:

```python
import pytest

from ray_dispatcher.models import Project, RemoteHost
from ray_dispatcher.provisioning import HostProvisioner, RemoteLayout, _StepError
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _prov(results, **kw):
    p = HostProvisioner(
        FakeTransport(run_results=results),
        Project(path="/local/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25"),
        RemoteHost(host="10.0.0.1", user="ubuntu"),
        runner_path="x", session_id="s", **kw,
    )
    p.layout = RemoteLayout("/home/ubuntu", "dfaas")
    return p


def _df(avail_kb):
    return f"/dev/sda1 100000000 1 {avail_kb} 1% /\n"


def test_preflight_ok():
    def results(argv):
        if argv[:2] == ["command", "-v"]:
            return CommandResult(0, f"/usr/bin/{argv[2]}", "", 0.0)
        if argv[0] == "sh" and "df -Pk" in argv[2]:
            return CommandResult(0, _df(10_000_000), "", 0.0)  # ~9.5 GB
        return CommandResult(0, "", "", 0.0)

    _prov(results)._preflight()  # no raise


def test_preflight_missing_tool_raises():
    def results(argv):
        if argv == ["command", "-v", "rsync"]:
            return CommandResult(1, "", "", 0.0)
        if argv[:2] == ["command", "-v"]:
            return CommandResult(0, "/usr/bin/python3", "", 0.0)
        return CommandResult(0, _df(10_000_000), "", 0.0)

    with pytest.raises(_StepError, match="rsync"):
        _prov(results)._preflight()


def test_preflight_low_disk_raises():
    def results(argv):
        if argv[:2] == ["command", "-v"]:
            return CommandResult(0, "/usr/bin/x", "", 0.0)
        if argv[0] == "sh" and "df -Pk" in argv[2]:
            return CommandResult(0, _df(1000), "", 0.0)  # < 1 MB
        return CommandResult(0, "", "", 0.0)

    with pytest.raises(_StepError, match="disk"):
        _prov(results, min_disk_mb=500)._preflight()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_provisioning_preflight.py -v`
Expected: FAIL with `AttributeError: 'HostProvisioner' object has no attribute '_preflight'`.

- [ ] **Step 3: Write the implementation (append to `HostProvisioner`)**

```python
    def _preflight(self) -> None:
        for tool in ("python3", "rsync"):
            if self.t.run(["command", "-v", tool]).returncode != 0:
                raise _StepError(f"required tool {tool!r} missing on {self.host.host}")
        root = shlex.quote(self._lo.root)
        # df -Pk gives POSIX one-line-per-fs output; available 1024-blocks is column 4.
        r = self._checked(
            ["sh", "-c", f"mkdir -p {root} && df -Pk {root} | tail -1"], "disk check"
        )
        try:
            avail_kb = int(r.stdout.split()[3])
        except (IndexError, ValueError) as exc:
            raise _StepError(f"could not parse disk free on {self.host.host}: {r.stdout!r}") from exc
        if avail_kb < self.min_disk_mb * 1024:
            raise _StepError(
                f"insufficient disk on {self.host.host}: "
                f"{avail_kb // 1024} MB < {self.min_disk_mb} MB required"
            )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_provisioning_preflight.py -v`
Expected: all cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/provisioning.py tests/unit/test_provisioning_preflight.py
git commit -m "feat: add provisioning preflight (python3/rsync/disk)"
```

---

### Task 3: `_install_uv` — versioned uv into dispatcher-owned path (§6.3.3)

**Files:**
- Modify: `src/ray_dispatcher/provisioning.py` (append method)
- Test: `tests/unit/test_provisioning_uv.py`

**Interfaces:**
- Consumes: `_checked`, `self._lo`, `self.force` (Task 1).
- Produces: `HostProvisioner._install_uv() -> str` — returns the absolute path to the version-specific `uv` binary. Skips installation when the binary already reports the exact version (unless `force`). After installing, verifies the version.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_provisioning_uv.py`:

```python
import pytest

from ray_dispatcher.models import Project, RemoteHost
from ray_dispatcher.provisioning import HostProvisioner, RemoteLayout, _StepError
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _prov(results, **kw):
    p = HostProvisioner(
        FakeTransport(run_results=results),
        Project(path="/local/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25"),
        RemoteHost(host="10.0.0.1", user="ubuntu"),
        runner_path="x", session_id="s", **kw,
    )
    p.layout = RemoteLayout("/home/ubuntu", "dfaas")
    return p


def _scripts(t):
    return [a[2] for a in (c[1] for c in t.calls if c[0] == "run") if a[0] == "sh"]


def test_install_uv_skips_when_present_and_correct():
    def results(argv):
        if argv[-1] == "--version":
            return CommandResult(0, "uv 0.11.25", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    p = _prov(results)
    uv = p._install_uv()
    assert uv == "/home/ubuntu/.ray_dispatcher/uv/0.11.25/uv"
    assert not any("astral.sh" in s for s in _scripts(p.t))  # no download


def test_install_uv_downloads_versioned_installer_then_verifies():
    state = {"installed": False}

    def results(argv):
        if argv[-1] == "--version":
            if state["installed"]:
                return CommandResult(0, "uv 0.11.25", "", 0.0)
            return CommandResult(1, "", "not found", 0.0)
        if argv[0] == "sh" and "astral.sh/uv/0.11.25/install.sh" in argv[2]:
            state["installed"] = True
            return CommandResult(0, "", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    p = _prov(results)
    p._install_uv()
    dl = [s for s in _scripts(p.t) if "astral.sh/uv/0.11.25/install.sh" in s][0]
    assert "UV_INSTALL_DIR=" in dl and "/uv/0.11.25" in dl and "INSTALLER_NO_MODIFY_PATH=1" in dl


def test_install_uv_force_redownloads_even_if_present():
    seen = {"download": False}

    def results(argv):
        if argv[-1] == "--version":
            return CommandResult(0, "uv 0.11.25", "", 0.0)
        if argv[0] == "sh" and "astral.sh" in argv[2]:
            seen["download"] = True
        return CommandResult(0, "", "", 0.0)

    _prov(results, force=True)._install_uv()
    assert seen["download"] is True


def test_install_uv_wrong_version_after_install_raises():
    def results(argv):
        if argv[-1] == "--version":
            return CommandResult(0, "uv 0.9.0", "", 0.0)  # never the wanted version
        return CommandResult(0, "", "", 0.0)

    with pytest.raises(_StepError, match="expected 0.11.25"):
        _prov(results, force=True)._install_uv()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_provisioning_uv.py -v`
Expected: FAIL with `AttributeError: ... '_install_uv'`.

- [ ] **Step 3: Write the implementation (append to `HostProvisioner`)**

```python
    def _install_uv(self) -> str:
        ver = self.project.uv_version
        uv = self._lo.uv_bin(ver)
        if not self.force and ver in self.t.run([uv, "--version"]).stdout:
            return uv
        install_dir = f"{self._lo.uv_root}/{ver}"
        # ponytail: official version-pinned installer; exact on-disk layout
        #           (UV_INSTALL_DIR -> <dir>/uv) is reconfirmed by the Phase 7 e2e.
        script = (
            f"set -e; mkdir -p {shlex.quote(install_dir)}; "
            f"curl -LsSf https://astral.sh/uv/{ver}/install.sh "
            f"| env UV_INSTALL_DIR={shlex.quote(install_dir)} INSTALLER_NO_MODIFY_PATH=1 sh"
        )
        self._checked(["sh", "-c", script], "uv install")
        reported = self._checked([uv, "--version"], "uv version check").stdout
        if ver not in reported:
            raise _StepError(
                f"installed uv on {self.host.host} reports {reported.strip()!r}, expected {ver}"
            )
        return uv
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_provisioning_uv.py -v`
Expected: all cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/provisioning.py tests/unit/test_provisioning_uv.py
git commit -m "feat: install exact uv into dispatcher-owned versioned path"
```

---

### Task 4: `_install_python` — `uv python install` + verify (§6.3.4)

**Files:**
- Modify: `src/ray_dispatcher/provisioning.py` (append method)
- Test: `tests/unit/test_provisioning_python.py`

**Interfaces:**
- Consumes: `_checked` (Task 1), the uv path from `_install_uv` (Task 3).
- Produces: `HostProvisioner._install_python(uv: str) -> None` — runs `uv python install <exact>`, resolves the interpreter via `uv python find <exact>`, runs it, and verifies the reported `X.Y.Z` matches exactly.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_provisioning_python.py`:

```python
import pytest

from ray_dispatcher.models import Project, RemoteHost
from ray_dispatcher.provisioning import HostProvisioner, RemoteLayout, _StepError
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _prov(results):
    p = HostProvisioner(
        FakeTransport(run_results=results),
        Project(path="/local/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25"),
        RemoteHost(host="10.0.0.1", user="ubuntu"),
        runner_path="x", session_id="s",
    )
    p.layout = RemoteLayout("/home/ubuntu", "dfaas")
    return p


UV = "/home/ubuntu/.ray_dispatcher/uv/0.11.25/uv"
INTERP = "/home/ubuntu/.local/share/uv/python/cpython-3.10.18/bin/python3"


def test_install_python_installs_finds_and_verifies():
    def results(argv):
        if argv[:3] == [UV, "python", "install"]:
            return CommandResult(0, "", "", 0.0)
        if argv[:3] == [UV, "python", "find"]:
            return CommandResult(0, INTERP + "\n", "", 0.0)
        if argv[0] == INTERP:
            return CommandResult(0, "3.10.18\n", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    p = _prov(results)
    p._install_python(UV)  # no raise
    argvs = [c[1] for c in p.t.calls if c[0] == "run"]
    assert (UV, "python", "install", "3.10.18") in argvs


def test_install_python_version_mismatch_raises():
    def results(argv):
        if argv[:3] == [UV, "python", "find"]:
            return CommandResult(0, INTERP, "", 0.0)
        if argv[0] == INTERP:
            return CommandResult(0, "3.10.5\n", "", 0.0)  # wrong patch
        return CommandResult(0, "", "", 0.0)

    with pytest.raises(_StepError, match="3.10.18"):
        _prov(results)._install_python(UV)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_provisioning_python.py -v`
Expected: FAIL with `AttributeError: ... '_install_python'`.

- [ ] **Step 3: Write the implementation (append to `HostProvisioner`)**

```python
    def _install_python(self, uv: str) -> None:
        want = self.project.python
        self._checked([uv, "python", "install", want], "uv python install")
        interp = self._checked([uv, "python", "find", want], "uv python find").stdout.strip()
        if not interp:
            raise _StepError(f"uv could not locate Python {want} on {self.host.host}")
        got = self._checked(
            [interp, "-c", "import sys;print('.'.join(map(str,sys.version_info[:3])))"],
            "python version check",
        ).stdout.strip()
        if got != want:
            raise _StepError(
                f"interpreter on {self.host.host} is {got!r}, expected {want!r}"
            )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_provisioning_python.py -v`
Expected: both cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/provisioning.py tests/unit/test_provisioning_python.py
git commit -m "feat: install and verify exact Python via uv"
```

---

### Task 5: `_sync_source` — rsync + atomic replace + source-manifest (§6.3.5)

**Files:**
- Modify: `src/ray_dispatcher/provisioning.py` (append method + import)
- Test: `tests/unit/test_provisioning_source.py`

**Interfaces:**
- Consumes: `_checked`, `_write_remote_file`, `self._lo` (Task 1); `Transport.push` (Phase 2); `source_digest` (Phase 3a).
- Produces: `HostProvisioner._sync_source() -> str` — rsyncs the local project tree into `<source>.staging` with `--delete` + `project.exclude`, atomically replaces `source/`, computes `source_digest` locally, writes `source-manifest.json` after success, and returns the digest.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_provisioning_source.py`:

```python
from ray_dispatcher.models import Project, RemoteHost
from ray_dispatcher.provisioning import HostProvisioner, RemoteLayout
from ray_dispatcher.ssh import FakeTransport


def _prov(tmp_path):
    (tmp_path / "run.py").write_text("print('x')")
    p = HostProvisioner(
        FakeTransport(),  # default rc 0
        Project(path=str(tmp_path), project_id="dfaas", python="3.10.18", uv_version="0.11.25"),
        RemoteHost(host="10.0.0.1", user="ubuntu"),
        runner_path="x", session_id="s",
    )
    p.layout = RemoteLayout("/home/ubuntu", "dfaas")
    return p


def _scripts(t):
    return [a[2] for a in (c[1] for c in t.calls if c[0] == "run") if a[0] == "sh"]


def test_sync_source_pushes_with_delete_and_excludes(tmp_path):
    p = _prov(tmp_path)
    digest = p._sync_source()
    pushes = [c[1] for c in p.t.calls if c[0] == "push"]
    assert len(pushes) == 1
    local, remote, delete, excludes = pushes[0]
    assert local.endswith("/")  # trailing slash -> copy contents
    assert remote.endswith("/source.staging/")
    assert delete is True
    assert tuple(excludes) == (".venv/", ".git/", "solutions/")
    assert len(digest) == 64


def test_sync_source_atomically_replaces_then_writes_manifest(tmp_path):
    p = _prov(tmp_path)
    p._sync_source()
    scripts = _scripts(p.t)
    replace = [s for s in scripts if "mv" in s and "/source.staging" in s][0]
    assert "/source.staging" in replace and "mv" in replace
    manifest = [s for s in scripts if "source-manifest.json.tmp" in s][0]
    assert "source_digest" in manifest
    # manifest is written AFTER the atomic replace
    assert scripts.index(manifest) > scripts.index(replace)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_provisioning_source.py -v`
Expected: FAIL with `AttributeError: ... '_sync_source'`.

- [ ] **Step 3: Write the implementation**

Add to the imports block of `provisioning.py`:

```python
import json

from .digests import source_digest
```

Append to `HostProvisioner`:

```python
    def _sync_source(self) -> str:
        staging = f"{self._lo.source}.staging"
        self._checked(["sh", "-c", f"mkdir -p {shlex.quote(staging)}"], "source staging mkdir")
        # trailing slashes: copy the *contents* of the local tree into staging.
        self.t.push(
            self.project.path.rstrip("/") + "/",
            staging + "/",
            delete=True,
            excludes=self.project.exclude,
        )
        src = shlex.quote(self._lo.source)
        stg = shlex.quote(staging)
        old = shlex.quote(self._lo.source + ".old")
        # The `mv staging source` rename is atomic within one parent dir; the brief
        # window where source is absent is acceptable pre-runtime (no readers yet).
        self._checked(
            ["sh", "-c", f"rm -rf {old}; if [ -e {src} ]; then mv {src} {old}; fi; "
                         f"mv {stg} {src}; rm -rf {old}"],
            "source atomic replace",
        )
        digest = source_digest(self.project.path, self.project.exclude)
        manifest = json.dumps({"source_digest": digest, "project_id": self.project.project_id})
        self._write_remote_file(self._lo.source_manifest, manifest)
        return digest
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_provisioning_source.py -v`
Expected: both cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/provisioning.py tests/unit/test_provisioning_source.py
git commit -m "feat: sync source with atomic replace + source-manifest"
```

---

### Task 6: `_publish_env` — uv sync + atomic publish + env-manifest (§6.3.6)

**Files:**
- Modify: `src/ray_dispatcher/provisioning.py` (append method)
- Test: `tests/unit/test_provisioning_env.py`

**Interfaces:**
- Consumes: `_checked`, `_write_remote_file`, `self._lo`, `self.force` (Task 1); `environment_digest` (Phase 3a); the uv path (Task 3); the published source (Task 5).
- Produces: `HostProvisioner._publish_env(uv: str) -> str` — probes the worker platform (`uname -sm`), computes `environment_digest`, skips when the env manifest + venv python already exist (unless `force`), otherwise `uv sync` into a staging `.venv` (with `UV_PROJECT_ENVIRONMENT`), interpreter smoke-checks, writes the env manifest, atomically publishes the env dir, and returns the digest. `SYNC_FLAGS` is the module-level tuple of base sync flags.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_provisioning_env.py`:

```python
from ray_dispatcher.models import Project, RemoteHost
from ray_dispatcher.provisioning import HostProvisioner, RemoteLayout, SYNC_FLAGS
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _prov(tmp_path, results, *, groups=(), **kw):
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n")
    (tmp_path / "uv.lock").write_text("# lock\n")
    p = HostProvisioner(
        FakeTransport(run_results=results),
        Project(path=str(tmp_path), project_id="dfaas", python="3.10.18",
                uv_version="0.11.25", dependency_groups=groups),
        RemoteHost(host="10.0.0.1", user="ubuntu"),
        runner_path="x", session_id="s", **kw,
    )
    p.layout = RemoteLayout("/home/ubuntu", "dfaas")
    return p


UV = "/home/ubuntu/.ray_dispatcher/uv/0.11.25/uv"


def _scripts(t):
    return [a[2] for a in (c[1] for c in t.calls if c[0] == "run") if a[0] == "sh"]


def test_publish_env_skips_when_already_valid(tmp_path):
    def results(argv):
        if argv[0] == "uname":
            return CommandResult(0, "Linux x86_64\n", "", 0.0)
        if argv[0] == "sh" and "test -f" in argv[2] and "environment-manifest.json" in argv[2]:
            return CommandResult(0, "", "", 0.0)  # already published & valid
        return CommandResult(0, "", "", 0.0)

    p = _prov(tmp_path, results)
    p._publish_env(UV)
    assert not any("uv" in s and "sync" in s for s in _scripts(p.t))  # no sync ran


def test_publish_env_syncs_smoke_checks_and_publishes(tmp_path):
    def results(argv):
        if argv[0] == "uname":
            return CommandResult(0, "Linux x86_64\n", "", 0.0)
        if argv[0] == "sh" and "test -f" in argv[2] and "environment-manifest" in argv[2]:
            return CommandResult(1, "", "", 0.0)  # not yet valid -> build it
        return CommandResult(0, "", "", 0.0)

    p = _prov(tmp_path, results, groups=("dev",))
    digest = p._publish_env(UV)
    scripts = _scripts(p.t)
    sync = [s for s in scripts if "uv" in s and "sync" in s][0]
    assert "UV_PROJECT_ENVIRONMENT=" in sync
    assert "--locked" in sync and "--no-install-project" in sync and "--no-default-groups" in sync
    assert "--group dev" in sync
    assert "--python 3.10.18" in sync
    smoke = [s for s in scripts if "import sys" in s][0]
    publish = [s for s in scripts if "mv" in s and "/envs/" in s and ".staging" in s][0]
    # smoke check happens before the atomic publish
    assert scripts.index(smoke) < scripts.index(publish)
    manifest = [s for s in scripts if "environment-manifest.json.tmp" in s][0]
    assert digest in manifest


def test_publish_env_force_rebuilds_even_if_valid(tmp_path):
    def results(argv):
        if argv[0] == "uname":
            return CommandResult(0, "Linux x86_64\n", "", 0.0)
        if argv[0] == "sh" and "test -f" in argv[2] and "environment-manifest" in argv[2]:
            return CommandResult(0, "", "", 0.0)  # would normally skip
        return CommandResult(0, "", "", 0.0)

    p = _prov(tmp_path, results, force=True)
    p._publish_env(UV)
    assert any("uv" in s and "sync" in s for s in _scripts(p.t))  # rebuilt anyway
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_provisioning_env.py -v`
Expected: FAIL with `ImportError: cannot import name 'SYNC_FLAGS'` (or `AttributeError: ... '_publish_env'`).

- [ ] **Step 3: Write the implementation**

Add to the imports block of `provisioning.py`:

```python
from .digests import environment_digest, source_digest
```

(replace the existing `from .digests import source_digest` line with the combined import above)

Add a module-level constant after the imports:

```python
# Base sync flags shared by environment_digest and the uv sync invocation (§6.3.6).
SYNC_FLAGS = (
    "--locked",
    "--no-install-project",
    "--no-install-workspace",
    "--no-default-groups",
)
```

Append to `HostProvisioner`:

```python
    def _publish_env(self, uv: str) -> str:
        platform = self._checked(["uname", "-sm"], "platform probe").stdout.strip()
        digest = environment_digest(self.project, platform=platform, sync_flags=SYNC_FLAGS)
        env_dir = self._lo.env_dir(digest)
        venv = self._lo.env_venv(digest)
        manifest_path = self._lo.env_manifest(digest)
        valid = self.t.run(
            ["sh", "-c", f"test -f {shlex.quote(manifest_path)} && "
                         f"test -x {shlex.quote(venv)}/bin/python"]
        ).returncode == 0
        if valid and not self.force:
            return digest

        staging = f"{env_dir}.staging"
        staging_venv = f"{staging}/.venv"
        self._checked(
            ["sh", "-c", f"rm -rf {shlex.quote(staging)}; mkdir -p {shlex.quote(staging)}"],
            "env staging mkdir",
        )
        sync = [uv, "sync", "--project", self._lo.source, *SYNC_FLAGS,
                "--python", self.project.python]
        for group in self.project.dependency_groups:
            sync += ["--group", group]
        self._checked(
            ["sh", "-c",
             f"UV_PROJECT_ENVIRONMENT={shlex.quote(staging_venv)} {shlex.join(sync)}"],
            "uv sync",
        )
        # ponytail: venv relocatability after the atomic move is reconfirmed by the
        #           Phase 7 e2e; bin/python is a symlink to the uv interpreter and
        #           survives a move, console-script shebangs would not (jobs use python).
        self._checked(
            ["sh", "-c", f"{shlex.quote(staging_venv)}/bin/python -c 'import sys'"],
            "venv smoke check",
        )
        manifest = json.dumps({
            "environment_digest": digest,
            "python": self.project.python,
            "uv_version": self.project.uv_version,
            "platform": platform,
            "dependency_groups": list(self.project.dependency_groups),
            "sync_flags": list(SYNC_FLAGS),
        })
        self._write_remote_file(f"{staging}/environment-manifest.json", manifest)
        qenv, qstg = shlex.quote(env_dir), shlex.quote(staging)
        qold = shlex.quote(env_dir + ".old")
        self._checked(
            ["sh", "-c", f"mkdir -p {shlex.quote(self._lo.project)}/envs; rm -rf {qold}; "
                         f"if [ -e {qenv} ]; then mv {qenv} {qold}; fi; "
                         f"mv {qstg} {qenv}; rm -rf {qold}"],
            "env atomic publish",
        )
        return digest
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_provisioning_env.py -v`
Expected: all three cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/provisioning.py tests/unit/test_provisioning_env.py
git commit -m "feat: publish uv-synced environment with atomic publish + manifest"
```

---

### Task 7: `_install_runner` — versioned remote runner (§6.3.7)

**Files:**
- Modify: `src/ray_dispatcher/provisioning.py` (append method + import)
- Test: `tests/unit/test_provisioning_runner.py`

**Interfaces:**
- Consumes: `_checked`, `self._lo`, `self.force` (Task 1); `Transport.push`; `runner_digest` (Phase 3a); `self.runner_path`.
- Produces: `HostProvisioner._install_runner() -> str` — computes `runner_digest(self.runner_path)`, skips when the versioned runner already exists (unless `force`), otherwise mkdir + pushes the runner, and returns the digest.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_provisioning_runner.py`:

```python
from ray_dispatcher.models import Project, RemoteHost
from ray_dispatcher.provisioning import HostProvisioner, RemoteLayout
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _prov(tmp_path, results, **kw):
    runner = tmp_path / "remote_runner.py"
    runner.write_text("print('runner')")
    p = HostProvisioner(
        FakeTransport(run_results=results),
        Project(path="/local/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25"),
        RemoteHost(host="10.0.0.1", user="ubuntu"),
        runner_path=str(runner), session_id="s", **kw,
    )
    p.layout = RemoteLayout("/home/ubuntu", "dfaas")
    return p


def test_install_runner_pushes_when_absent(tmp_path):
    def results(argv):
        if argv[0] == "test" and argv[1] == "-f":
            return CommandResult(1, "", "", 0.0)  # absent
        return CommandResult(0, "", "", 0.0)

    p = _prov(tmp_path, results)
    digest = p._install_runner()
    pushes = [c[1] for c in p.t.calls if c[0] == "push"]
    assert len(pushes) == 1
    local, remote, _, _ = pushes[0]
    assert remote == f"/home/ubuntu/.ray_dispatcher/bin/{digest}/remote_runner.py"


def test_install_runner_skips_when_present(tmp_path):
    def results(argv):
        if argv[0] == "test" and argv[1] == "-f":
            return CommandResult(0, "", "", 0.0)  # present
        return CommandResult(0, "", "", 0.0)

    p = _prov(tmp_path, results)
    p._install_runner()
    assert not [c for c in p.t.calls if c[0] == "push"]


def test_install_runner_force_pushes_even_if_present(tmp_path):
    def results(argv):
        if argv[0] == "test" and argv[1] == "-f":
            return CommandResult(0, "", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    p = _prov(tmp_path, results, force=True)
    p._install_runner()
    assert [c for c in p.t.calls if c[0] == "push"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_provisioning_runner.py -v`
Expected: FAIL with `AttributeError: ... '_install_runner'`.

- [ ] **Step 3: Write the implementation**

Add to the imports block of `provisioning.py` (combine with the existing digests import):

```python
from .digests import environment_digest, runner_digest, source_digest
```

Append to `HostProvisioner`:

```python
    def _install_runner(self) -> str:
        digest = runner_digest(self.runner_path)
        remote = self._lo.runner(digest)
        present = self.t.run(["test", "-f", remote]).returncode == 0
        if present and not self.force:
            return digest
        self._checked(
            ["sh", "-c", f"mkdir -p {shlex.quote(self._lo.runner_dir(digest))}"],
            "runner dir mkdir",
        )
        self.t.push(self.runner_path, remote)
        return digest
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_provisioning_runner.py -v`
Expected: all three cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/provisioning.py tests/unit/test_provisioning_runner.py
git commit -m "feat: install versioned remote runner"
```

---

### Task 8: `_copy_secrets` — copy with modes + verify ownership (§6.3.8)

**Files:**
- Modify: `src/ray_dispatcher/provisioning.py` (append method)
- Test: `tests/unit/test_provisioning_secrets.py`

**Interfaces:**
- Consumes: `_checked`, `self._lo`, `self.host`, `self.project.secrets` (Task 1); `Transport.push`.
- Produces: `HostProvisioner._copy_secrets() -> None` — no-op when there are no secrets; otherwise creates a `0700` secrets dir, pushes each secret, `chmod`s it to its declared mode, and verifies its owner is the SSH user without printing contents. Raises `_StepError` on an ownership mismatch.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_provisioning_secrets.py`:

```python
import pytest

from ray_dispatcher.models import Project, RemoteHost, SecretFile
from ray_dispatcher.provisioning import HostProvisioner, RemoteLayout, _StepError
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _prov(results, secrets, *, user="ubuntu"):
    p = HostProvisioner(
        FakeTransport(run_results=results),
        Project(path="/local/proj", project_id="dfaas", python="3.10.18",
                uv_version="0.11.25", secrets=secrets),
        RemoteHost(host="10.0.0.1", user=user),
        runner_path="x", session_id="s",
    )
    p.layout = RemoteLayout("/home/ubuntu", "dfaas")
    return p


def _scripts(t):
    return [a[2] for a in (c[1] for c in t.calls if c[0] == "run") if a[0] == "sh"]


def test_copy_secrets_noop_when_none():
    p = _prov(lambda a: CommandResult(0, "", "", 0.0), secrets=())
    p._copy_secrets()
    assert not p.t.calls


def test_copy_secrets_pushes_chmods_and_verifies_owner():
    def results(argv):
        if argv[0] == "sh" and "stat" in argv[2]:
            return CommandResult(0, "ubuntu 600\n", "", 0.0)  # owner matches, mode 600
        return CommandResult(0, "", "", 0.0)

    secrets = (SecretFile(source="/local/token", remote_name="token", mode=0o600),)
    p = _prov(results, secrets=secrets)
    p._copy_secrets()
    pushes = [c[1] for c in p.t.calls if c[0] == "push"]
    assert pushes[0][1] == "/home/ubuntu/.ray_dispatcher/secrets/dfaas/token"
    chmods = [c[1] for c in p.t.calls if c[0] == "run" and c[1][0] == "chmod"]
    assert ("chmod", "600", "/home/ubuntu/.ray_dispatcher/secrets/dfaas/token") == \
        (chmods[0][0], chmods[0][1], chmods[0][2])
    assert any("mkdir -p" in s and "chmod 700" in s for s in _scripts(p.t))  # 0700 dir
    # contents are never printed: no `cat`/`printf` of the secret file itself
    assert not any("cat /home/ubuntu/.ray_dispatcher/secrets" in s for s in _scripts(p.t))


def test_copy_secrets_wrong_owner_raises():
    def results(argv):
        if argv[0] == "sh" and "stat" in argv[2]:
            return CommandResult(0, "root 600\n", "", 0.0)  # owner mismatch
        return CommandResult(0, "", "", 0.0)

    secrets = (SecretFile(source="/local/token", remote_name="token"),)
    with pytest.raises(_StepError, match="owned by"):
        _prov(results, secrets=secrets, user="ubuntu")._copy_secrets()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_provisioning_secrets.py -v`
Expected: FAIL with `AttributeError: ... '_copy_secrets'`.

- [ ] **Step 3: Write the implementation (append to `HostProvisioner`)**

```python
    def _copy_secrets(self) -> None:
        if not self.project.secrets:
            return
        sdir = shlex.quote(self._lo.secrets)
        self._checked(
            ["sh", "-c", f"mkdir -p {sdir} && chmod 700 {sdir}"], "secrets dir"
        )
        for secret in self.project.secrets:
            remote = f"{self._lo.secrets}/{secret.remote_name}"
            self.t.push(secret.source, remote)
            self._checked(["chmod", f"{secret.mode:o}", remote], f"chmod secret {secret.remote_name}")
            # Owner check only — never reads the secret's contents (§6.3.8).
            # GNU stat first, BSD/macOS stat as a dev fallback.
            qr = shlex.quote(remote)
            owner = self._checked(
                ["sh", "-c", f"stat -c '%U' {qr} 2>/dev/null || stat -f '%Su' {qr}"],
                f"verify secret {secret.remote_name}",
            ).stdout.strip()
            if owner and owner != self.host.user:
                raise _StepError(
                    f"secret {secret.remote_name!r} on {self.host.host} owned by "
                    f"{owner!r}, expected {self.host.user!r}"
                )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_provisioning_secrets.py -v`
Expected: all three cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/provisioning.py tests/unit/test_provisioning_secrets.py
git commit -m "feat: copy secrets with modes and verify ownership"
```

---

### Task 9: `HostProvisioner.provision()` — driver: lock + heartbeat + steps (§6.3.1)

**Files:**
- Modify: `src/ray_dispatcher/provisioning.py` (append method + imports)
- Test: `tests/unit/test_provisioning_host.py`

**Interfaces:**
- Consumes: all `_*` steps (Tasks 2-8); `SessionLock`/`HeartbeatThread` (Phase 3a); `HostInUseError` (Phase 1); `HostProvisioningResult` (Phase 1).
- Produces: `HostProvisioner.provision() -> tuple[HostProvisioningResult, tuple[SessionLock, HeartbeatThread] | None]`:
  - acquires the session lock (returns a failed result on `HostInUseError`, with no live session),
  - starts the heartbeat, runs steps 2-8 (resolve layout → preflight → uv → python → source → env → runner → secrets),
  - on success: returns `(succeeded result, (lock, heartbeat))` — caller keeps them live,
  - on any step failure: stops the heartbeat **before** releasing the lock, returns `(failed result, None)`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_provisioning_host.py`:

```python
from ray_dispatcher.errors import HostInUseError
from ray_dispatcher.models import Project, RemoteHost
from ray_dispatcher.provisioning import HostProvisioner
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _ok_results(tmp_path):
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n")
    (tmp_path / "uv.lock").write_text("# lock\n")
    (tmp_path / "run.py").write_text("print('x')")

    def results(argv):
        joined = " ".join(argv)
        if "$HOME" in joined:
            return CommandResult(0, "/home/ubuntu", "", 0.0)
        if argv[:2] == ["command", "-v"]:
            return CommandResult(0, "/usr/bin/x", "", 0.0)
        if "df -Pk" in joined:
            return CommandResult(0, "/dev/sda1 100 1 999999999 1% /\n", "", 0.0)
        if argv[-1] == "--version":
            return CommandResult(0, "uv 0.11.25", "", 0.0)
        if argv[1:3] == ["python", "find"]:
            return CommandResult(0, "/interp", "", 0.0)
        if argv[0] == "/interp":
            return CommandResult(0, "3.10.18", "", 0.0)
        if argv[0] == "uname":
            return CommandResult(0, "Linux x86_64", "", 0.0)
        if "stat" in joined:
            return CommandResult(0, "ubuntu", "", 0.0)
        if "import sys" in joined:
            return CommandResult(0, "", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    return results


def _prov(tmp_path, results, runner_path):
    return HostProvisioner(
        FakeTransport(run_results=results),
        Project(path=str(tmp_path), project_id="dfaas", python="3.10.18", uv_version="0.11.25"),
        RemoteHost(host="10.0.0.1", user="ubuntu"),
        runner_path=runner_path, session_id="sess-1", heartbeat_interval_s=1000.0,
    )


def test_provision_success_returns_live_session(tmp_path):
    runner = tmp_path / "remote_runner.py"
    runner.write_text("print('r')")
    p = _prov(tmp_path, _ok_results(tmp_path), str(runner))
    result, session = p.provision()
    assert result.succeeded is True
    assert result.host == "10.0.0.1"
    assert len(result.source_digest) == 64 and len(result.environment_digest) == 64
    assert session is not None
    lock, hb = session
    hb.stop()  # test cleanup
    assert lock.session_id == "sess-1"


def test_provision_step_failure_releases_lock_and_marks_failed(tmp_path):
    runner = tmp_path / "remote_runner.py"
    runner.write_text("print('r')")

    def failing(argv):
        if argv[:2] == ["command", "-v"]:
            return CommandResult(1, "", "missing", 0.0)  # preflight fails
        if "$HOME" in " ".join(argv):
            return CommandResult(0, "/home/ubuntu", "", 0.0)
        if argv[0] == "sh" and "cat" in argv[2]:  # SessionLock._read_owner: we own it
            return CommandResult(0, '{"session_id": "sess-1", "heartbeat": 0}', "", 0.0)
        return CommandResult(0, "", "", 0.0)

    p = _prov(tmp_path, failing, str(runner))
    result, session = p.provision()
    assert result.succeeded is False
    assert session is None
    assert "missing" in result.error or "python3" in result.error
    # the lock was released after the failure
    assert any(c[0] == "run" and c[1][0] == "sh" and "rm -f" in c[1][2] for c in p.t.calls)


def test_provision_host_in_use_returns_failed_no_session(tmp_path):
    runner = tmp_path / "remote_runner.py"
    runner.write_text("print('r')")

    def held(argv):
        joined = " ".join(argv)
        if "set -C" in joined:
            return CommandResult(1, "", "", 0.0)  # lock exists
        if argv[0] == "sh" and "cat" in argv[2]:
            return CommandResult(0, '{"session_id": "other", "heartbeat": 9e18}', "", 0.0)
        return CommandResult(0, "", "", 0.0)

    p = HostProvisioner(
        FakeTransport(run_results=held),
        Project(path=str(tmp_path), project_id="dfaas", python="3.10.18", uv_version="0.11.25"),
        RemoteHost(host="10.0.0.1", user="ubuntu"),
        runner_path=str(runner), session_id="sess-1",
    )
    result, session = p.provision()
    assert result.succeeded is False and session is None
    assert "lock" in result.error.lower() or "use" in result.error.lower()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_provisioning_host.py -v`
Expected: FAIL with `AttributeError: 'HostProvisioner' object has no attribute 'provision'`.

- [ ] **Step 3: Write the implementation**

Add to the imports block of `provisioning.py`:

```python
from .errors import HostInUseError
from .locking import HeartbeatThread, SessionLock
from .models import HostProvisioningResult, Project, RemoteHost
```

(merge the `HostProvisioningResult` into the existing `from .models import ...` line)

Append to `HostProvisioner`:

```python
    def provision(self) -> tuple[HostProvisioningResult, tuple[SessionLock, HeartbeatThread] | None]:
        lock = SessionLock(self.t, self.session_id)
        try:
            lock.acquire()
        except HostInUseError as exc:
            return HostProvisioningResult(self.host.host, False, None, None, error=str(exc)), None
        hb = HeartbeatThread(lock, interval_s=self.heartbeat_interval_s)
        hb.start()
        source_dig: str | None = None
        env_dig: str | None = None
        try:
            self.layout = self._resolve_layout()
            self._preflight()
            uv = self._install_uv()
            self._install_python(uv)
            source_dig = self._sync_source()
            env_dig = self._publish_env(uv)
            self._install_runner()
            self._copy_secrets()
        except Exception as exc:  # noqa: BLE001 — any step failure marks the host unhealthy
            hb.stop()  # stop the heartbeat BEFORE releasing (avoids a beat/rm race)
            lock.release()
            return HostProvisioningResult(
                self.host.host, False, source_dig, env_dig, error=str(exc)
            ), None
        return HostProvisioningResult(self.host.host, True, source_dig, env_dig), (lock, hb)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_provisioning_host.py -v`
Expected: all three cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/provisioning.py tests/unit/test_provisioning_host.py
git commit -m "feat: HostProvisioner.provision driver (lock + heartbeat + steps)"
```

---

### Task 10: inventory `provision()` + `ProvisioningOutcome` (§6.3 orchestration)

**Files:**
- Modify: `src/ray_dispatcher/provisioning.py` (append dataclass + function + imports)
- Test: `tests/unit/test_provisioning_inventory.py`

**Interfaces:**
- Consumes: `HostProvisioner` (Tasks 1-9); `Inventory`/`ProvisioningReport` (Phase 1); `SshConfig`/`SshTransport` (Phase 2); `ProvisioningError`/`NoHealthyHostsError` (Phase 1).
- Produces:
  - `ProvisioningOutcome` (mutable dataclass): `report: ProvisioningReport`, `sessions: dict[str, tuple[SessionLock, HeartbeatThread]]`, method `release_all() -> None` (stops each heartbeat then releases each lock, then clears).
  - `provision(inventory, project, *, runner_path, require_all_hosts=False, force=False, transport_factory=None, session_id=None, min_disk_mb=500) -> ProvisioningOutcome` — runs hosts in a thread pool bounded by inventory size, builds the report in inventory order, keeps live sessions for healthy hosts, and applies `require_all_hosts`/`≥1-healthy` policy (releasing all locks before raising).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_provisioning_inventory.py`:

```python
import pytest

from ray_dispatcher.errors import NoHealthyHostsError, ProvisioningError
from ray_dispatcher.models import Inventory, Project, RemoteHost
from ray_dispatcher.provisioning import ProvisioningOutcome, provision
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _project(tmp_path):
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n")
    (tmp_path / "uv.lock").write_text("# lock\n")
    (tmp_path / "run.py").write_text("print('x')")
    return Project(path=str(tmp_path), project_id="dfaas", python="3.10.18", uv_version="0.11.25")


def _ok(argv):
    joined = " ".join(argv)
    if "$HOME" in joined:
        return CommandResult(0, "/home/ubuntu", "", 0.0)
    if argv[:2] == ["command", "-v"]:
        return CommandResult(0, "/usr/bin/x", "", 0.0)
    if "df -Pk" in joined:
        return CommandResult(0, "/dev/sda1 100 1 999999999 1% /\n", "", 0.0)
    if argv[-1] == "--version":
        return CommandResult(0, "uv 0.11.25", "", 0.0)
    if argv[1:3] == ["python", "find"]:
        return CommandResult(0, "/interp", "", 0.0)
    if argv[0] == "/interp":
        return CommandResult(0, "3.10.18", "", 0.0)
    if argv[0] == "uname":
        return CommandResult(0, "Linux x86_64", "", 0.0)
    return CommandResult(0, "", "", 0.0)


def _fail(argv):
    if argv[:2] == ["command", "-v"]:
        return CommandResult(1, "", "tool missing", 0.0)
    return _ok(argv)


def _runner(tmp_path):
    r = tmp_path / "remote_runner.py"
    r.write_text("print('r')")
    return str(r)


def test_provision_all_healthy_keeps_sessions(tmp_path):
    inv = Inventory((RemoteHost(host="a", user="ubuntu"), RemoteHost(host="b", user="ubuntu")))
    outcome = provision(
        inv, _project(tmp_path), runner_path=_runner(tmp_path),
        transport_factory=lambda h: FakeTransport(run_results=_ok),
    )
    assert isinstance(outcome, ProvisioningOutcome)
    assert [r.host for r in outcome.report.hosts] == ["a", "b"]  # inventory order
    assert all(r.succeeded for r in outcome.report.hosts)
    assert set(outcome.sessions) == {"ubuntu@a:22", "ubuntu@b:22"}
    outcome.release_all()  # cleanup heartbeats
    assert outcome.sessions == {}


def test_provision_partial_failure_proceeds_with_healthy(tmp_path):
    inv = Inventory((RemoteHost(host="good", user="ubuntu"), RemoteHost(host="bad", user="ubuntu")))

    def factory(h):
        return FakeTransport(run_results=_ok if h.host == "good" else _fail)

    outcome = provision(inv, _project(tmp_path), runner_path=_runner(tmp_path),
                        transport_factory=factory)
    by_host = {r.host: r for r in outcome.report.hosts}
    assert by_host["good"].succeeded and not by_host["bad"].succeeded
    assert set(outcome.sessions) == {"ubuntu@good:22"}  # only the healthy host kept
    outcome.release_all()


def test_require_all_hosts_aborts_and_releases(tmp_path):
    inv = Inventory((RemoteHost(host="good", user="ubuntu"), RemoteHost(host="bad", user="ubuntu")))

    def factory(h):
        return FakeTransport(run_results=_ok if h.host == "good" else _fail)

    with pytest.raises(ProvisioningError) as ei:
        provision(inv, _project(tmp_path), runner_path=_runner(tmp_path),
                  transport_factory=factory, require_all_hosts=True)
    assert ei.value.report is not None
    assert len(ei.value.report.hosts) == 2  # complete report collected before abort


def test_no_healthy_hosts_raises(tmp_path):
    inv = Inventory((RemoteHost(host="bad", user="ubuntu"),))
    with pytest.raises(NoHealthyHostsError):
        provision(inv, _project(tmp_path), runner_path=_runner(tmp_path),
                  transport_factory=lambda h: FakeTransport(run_results=_fail))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_provisioning_inventory.py -v`
Expected: FAIL with `ImportError: cannot import name 'provision'`.

- [ ] **Step 3: Write the implementation**

Add to the imports block of `provisioning.py`:

```python
import secrets as _secrets
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from .errors import HostInUseError, NoHealthyHostsError, ProvisioningError
from .models import HostProvisioningResult, Inventory, Project, ProvisioningReport, RemoteHost
from .ssh import CommandResult, SshConfig, SshTransport, Transport
```

(merge `Inventory`/`ProvisioningReport` into the existing models import, and `NoHealthyHostsError`/`ProvisioningError` into the existing errors import; merge `SshConfig`/`SshTransport` into the existing ssh import)

Append to `provisioning.py` (after `HostProvisioner`):

```python
def _label(host: RemoteHost) -> str:
    return f"{host.user}@{host.host}:{host.port}"


@dataclass
class ProvisioningOutcome:
    """Report plus the live session locks for healthy hosts (held until teardown)."""

    report: ProvisioningReport
    sessions: dict[str, tuple[SessionLock, HeartbeatThread]] = field(default_factory=dict)

    def release_all(self) -> None:
        for lock, hb in self.sessions.values():
            hb.stop()  # stop the heartbeat before releasing (avoids a beat/rm race)
            lock.release()
        self.sessions = {}


def _default_transport(host: RemoteHost) -> Transport:
    return SshTransport(SshConfig.from_host(host))


def provision(
    inventory: Inventory,
    project: Project,
    *,
    runner_path: str,
    require_all_hosts: bool = False,
    force: bool = False,
    transport_factory: Callable[[RemoteHost], Transport] | None = None,
    session_id: str | None = None,
    min_disk_mb: int = 500,
) -> ProvisioningOutcome:
    factory = transport_factory or _default_transport
    sid = session_id or _secrets.token_hex(16)

    def work(host: RemoteHost) -> tuple[
        HostProvisioningResult, tuple[SessionLock, HeartbeatThread] | None
    ]:
        prov = HostProvisioner(
            factory(host), project, host,
            runner_path=runner_path, session_id=sid, force=force, min_disk_mb=min_disk_mb,
        )
        return prov.provision()

    results: dict[str, tuple[HostProvisioningResult, tuple[SessionLock, HeartbeatThread] | None]] = {}
    with ThreadPoolExecutor(max_workers=len(inventory.hosts)) as pool:
        futures = {pool.submit(work, h): h for h in inventory.hosts}
        for fut in futures:
            host = futures[fut]
            results[_label(host)] = fut.result()

    report = ProvisioningReport(tuple(results[_label(h)][0] for h in inventory.hosts))
    # Keep only the live sessions of healthy hosts. An explicit loop lets mypy
    # narrow `sess` to a concrete tuple inside the `if` (a dict comprehension would not).
    sessions: dict[str, tuple[SessionLock, HeartbeatThread]] = {}
    for host in inventory.hosts:
        sess = results[_label(host)][1]
        if sess is not None:
            sessions[_label(host)] = sess
    outcome = ProvisioningOutcome(report, sessions)

    healthy = [r for r in report.hosts if r.succeeded]
    if require_all_hosts and len(healthy) != len(inventory.hosts):
        outcome.release_all()
        raise ProvisioningError(report, "require_all_hosts=True: not every host provisioned")
    if not healthy:
        outcome.release_all()
        raise NoHealthyHostsError("no host provisioned successfully")
    return outcome
```

> Implementer note: iterating `futures` (a dict keyed by Future) blocks on each `fut.result()` in submission order, which is sufficient here — every future is awaited before the report is built. `as_completed` is not needed since the report is assembled after all hosts finish. Keep the `min_disk_mb` plumb-through so callers can tune the disk gate.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_provisioning_inventory.py -v`
Expected: all four cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/provisioning.py tests/unit/test_provisioning_inventory.py
git commit -m "feat: inventory provision() with ProvisioningOutcome + require_all_hosts policy"
```

---

### Task 11: Phase 3b gate

**Files:**
- (no new source) — final full-toolchain verification.

**Interfaces:** none.

- [ ] **Step 1: Run the full suite**

Run: `uv run pytest -q`
Expected: all tests `passed` (Phases 1, 2, 3a + Phase 3b).

- [ ] **Step 2: Lint + type check**

Run: `uv run ruff check --fix . && uv run ruff check . && uv run mypy`
Expected: ruff auto-sorts then reports `All checks passed!`; mypy reports `Success: no issues found`.

If ruff/mypy flags anything in `provisioning.py`, fix it minimally (no behavior change, no config relaxation, no blanket `# type: ignore`). In particular, confirm `HostProvisioner.provision`'s `tuple[..., tuple[SessionLock, HeartbeatThread] | None]` return type and the `ProvisioningOutcome.sessions` dict type check cleanly under strict mypy.

- [ ] **Step 3: Commit (only if Step 2 made changes)**

```bash
git add -A
git commit -m "chore: phase 3b gate green (ruff + mypy)"
```

---

## Phase 3b self-review

Run before declaring Phase 3b done:

- [ ] Remote paths match §6.1 exactly (`bin/<runner_digest>/`, `projects/<project_id>/{source,source-manifest.json,envs/<environment_digest>/{.venv,environment-manifest.json}}`, `secrets/<project_id>/`); `project_id` is stable, not a hash.
- [ ] All eight §6.3 steps are present and ordered: lock+heartbeat → preflight → uv → python → source(+manifest) → env(+manifest) → runner → secrets.
- [ ] uv is installed to a dispatcher-owned version-specific path and its version is verified; the system uv is never used. (§6.3.3)
- [ ] `uv sync` uses exactly `--locked --no-install-project --no-install-workspace --no-default-groups --python <exact>` plus `--group` per explicit group, with `UV_PROJECT_ENVIRONMENT` set to the staging venv; env published atomically only after smoke check. (§6.3.6)
- [ ] Source and environment digests are computed from the same inputs the digests cover; the same `SYNC_FLAGS` feed both `environment_digest` and the `uv sync` call (no drift).
- [ ] Secrets are pushed with declared modes, the dir is `0700`, ownership is verified, and no secret content is ever printed; secrets never enter any digest. (§6.3.8, §6.2)
- [ ] `force=True` re-runs the skip-gated steps (uv, env, runner) but keeps atomic publication; `require_all_hosts=True` collects a full report then aborts and releases all locks; `<1 healthy` raises `NoHealthyHostsError`. (§6.3)
- [ ] Every remote command embeds only library-controlled values and `shlex.quote`s interpolated data; no user/job string is shelled. (§7)
- [ ] On host failure the heartbeat is stopped before the lock is released; on success the live `(lock, heartbeat)` is retained in `ProvisioningOutcome.sessions`. (§3.2.6 + Phase 3a note)
- [ ] `uv run pytest -q`, `uv run ruff check .`, and `uv run mypy` all green.

**Deliverable:** `provisioning.py` — `RemoteLayout`, `HostProvisioner` (the full §6.3 per-host algorithm), `provision()` (bounded-parallel inventory orchestration), and `ProvisioningOutcome` (report + held session locks). Unit-tested end to end with `FakeTransport`.

**Residuals carried to later phases (documented):**
- Real-world calibration deferred to **Phase 7 e2e**: the exact uv installer on-disk layout (`UV_INSTALL_DIR` → `<dir>/uv`) and venv relocatability after the atomic move. Unit tests pin the command contract; the e2e confirms reality and adjusts paths/`--relocatable` if needed.
- **Phase 4**: full stale-lock reconciliation (probe for a live runner before takeover) and the session-long ownership of `ProvisioningOutcome.sessions` (the Dispatcher holds them across execution and calls `release_all()` at teardown).
