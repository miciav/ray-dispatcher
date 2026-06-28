# ray-dispatcher — Phase 1: Foundations — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the validated data layer and project scaffolding of `ray_dispatcher` — every public value object, the exception hierarchy, and path-safety helpers — fully tested without VMs or Ray.

**Architecture:** A `src/` layout package managed by uv. Pure value objects (`models.py`) validate themselves at construction and raise the typed errors in `errors.py`; all path/containment safety lives in one module (`paths.py`) reused by the models. No SSH, Ray, or filesystem-of-VM concerns appear in this phase — it is the foundation later phases consume.

**Tech Stack:** Python ≥3.10, uv, hatchling, pytest, ruff, mypy, PyYAML.

**Spec:** `docs/superpowers/specs/2026-06-27-ray-dispatcher-design.md` (sections referenced inline as §N).

## Global Constraints

Every task implicitly includes these. Values are copied verbatim from the spec.

- `requires-python = ">=3.10"` (§12). Use `str | None`, `tuple[...]`, builtin generics.
- Public value objects are **frozen dataclasses** (§4). Validation happens at construction and raises `ModelValidationError` (structural) or `PathValidationError` (path) — both subclasses of `ConfigurationError` → `DispatcherError` (§9.3).
- Job `id` must match `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$` and be unique within a batch (§4.3).
- Commands cannot be empty or contain NUL bytes; env keys must be valid POSIX variable names; `cwd`, input destinations, and output paths cannot be absolute, contain `..`, or resolve through a symlink outside the run root (§4.3).
- `Project.python` and `Project.uv_version` must be **exact** versions, e.g. `"3.10.18"`, `"0.11.25"` (§4.2).
- `SecretFile.remote_name` is one normalized filename, **not a path**; default `mode = 0o600`; secrets are never included in digests, results, or logs (§4.2, §6.2). Phase 1 only enforces the structural rules; redaction is enforced where logs/digests are produced (Phase 3+).
- `RetryPolicy` default: `max_attempts = 2`, `retry_on = {SSH, HOST_LOST, COLLECTION}`; `max_attempts` must be ≥ 1 (§4.4, §8.3).
- `Inventory` validation rejects empty inventories, duplicate `(host, port, user)` entries, and non-positive slots (§4.1). File-existence validation of `identity_file`/`known_hosts_file` is performed at SSH-config resolution (Phase 2), not at model construction, so models stay filesystem-independent. Host key checking is always enabled; no password SSH (§3.1, §13).
- `Project.exclude` default is `(".venv/", ".git/", "solutions/")` (§4.2).

## Full-project file structure (for context; only Phase 1 files are built here)

```text
src/ray_dispatcher/
├── __init__.py          # public exports                       [Phase 1]
├── errors.py            # public exception hierarchy            [Phase 1]
├── paths.py             # normalization + containment checks    [Phase 1]
├── models.py            # validated public value objects        [Phase 1]
├── ssh.py               # Fabric + rsync transport              [Phase 2]
├── remote_runner.py     # versioned remote subprocess supervisor[Phase 2]
├── provisioning.py      # manifests, digests, uv/python, sync   [Phase 3]
├── scheduling.py        # Ray task, HostLease actor, registry   [Phase 4]
├── results.py           # attempt layout, collection, manifest  [Phase 5]
├── backends/
│   ├── base.py          # ExecutionBackend ABC                  [Phase 5]
│   └── ssh_ray.py       # exclusive local-Ray backend           [Phase 6]
└── dispatcher.py        # public lifecycle + batch orchestration [Phase 6]
```

### Phase roadmap (each phase = its own detailed plan, written when its turn comes)

1. **Foundations (this plan):** scaffolding, `errors.py`, `paths.py`, `models.py`. Deliverable: tested value/validation layer; `uv run pytest` green.
2. **SSH transport + remote runner:** `ssh.py` (Fabric + rsync behind a thin interface with a fake), SSH-config resolution incl. file-existence checks, `remote_runner.py` (manifest-driven `subprocess.Popen`, process-group control, log forwarding). Deliverable: transport + runner unit-tested with fakes and a local subprocess.
3. **Provisioning + cache invalidation:** source/environment/runner digests, atomic manifest publication, uv/Python install, `uv sync` flags, secrets, heartbeat session lock (§6, §3.2.6). Deliverable: provisioning unit-tested with fakes.
4. **Scheduling:** Ray task wrapper, async `HostLease` actor (capacity, tokens, expiry, quarantine, reconciliation), status registry; local-Ray integration tests (§3.2, §7.1, §8). Deliverable: concurrency/lease semantics proven on a local Ray runtime.
5. **Results + attempt execution + backend base:** `results.py` (attempt layout, collection, `result.json`), the `ExecutionBackend` ABC, and the per-attempt protocol (§7, §9.1). Deliverable: attempt execution + collection tested with fakes.
6. **Backend + Dispatcher:** `SshRayBackend` (exclusive runtime invariants), `dispatcher.py` (lifecycle, batch orchestration, `as_completed`, teardown), error contract (§3.2, §4.5, §9.3, §10). Deliverable: full library usable end-to-end against fakes.
7. **Multipass e2e:** opt-in `@pytest.mark.e2e` suite using `multipass-sdk` as a VM factory (§11). Deliverable: real round-trip on local VMs.

---

## Phase 1 file structure

- Create: `pyproject.toml`, `README.md`, `.gitignore`
- Create: `src/ray_dispatcher/__init__.py`
- Create: `src/ray_dispatcher/errors.py`
- Create: `src/ray_dispatcher/paths.py`
- Create: `src/ray_dispatcher/models.py`
- Create: `tests/unit/test_smoke.py`, `tests/unit/test_errors.py`, `tests/unit/test_paths.py`, `tests/unit/test_models_hosts.py`, `tests/unit/test_models_project.py`, `tests/unit/test_models_jobs.py`, `tests/unit/test_models_results.py`, `tests/unit/test_public_api.py`

---

### Task 1: Project scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `README.md`
- Create: `.gitignore`
- Create: `src/ray_dispatcher/__init__.py`
- Test: `tests/unit/test_smoke.py`

**Interfaces:**
- Consumes: nothing.
- Produces: an installable `ray_dispatcher` package exposing `__version__: str`; a working `uv run pytest`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_smoke.py`:

```python
import ray_dispatcher


def test_package_exposes_version():
    assert isinstance(ray_dispatcher.__version__, str)
    assert ray_dispatcher.__version__
```

- [ ] **Step 2: Create the package and config**

`.gitignore`:

```text
.venv/
__pycache__/
*.pyc
.pytest_cache/
.mypy_cache/
.ruff_cache/
dist/
.coverage
results/
```

`README.md`:

```markdown
# ray-dispatcher

Generic, Ray-scheduled dispatcher for subprocess jobs on SSH-accessible VMs.
See `docs/superpowers/specs/` for the design and `docs/superpowers/plans/` for
the implementation plan.
```

`pyproject.toml`:

```toml
[project]
name = "ray-dispatcher"
version = "0.1.0"
description = "Generic Ray-scheduled dispatcher for subprocess jobs on SSH-accessible VMs."
readme = "README.md"
requires-python = ">=3.10"
dependencies = [
  "ray>=2.40,<3",
  "fabric>=3.2,<4",
  "pyyaml>=6.0,<7",
]

[project.optional-dependencies]
progress = ["rich>=13,<15"]

[dependency-groups]
dev = [
  "pytest>=8,<9",
  "pytest-cov>=5",
  "mypy>=1.10",
  "ruff>=0.5",
  "types-pyyaml",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/ray_dispatcher"]

[tool.pytest.ini_options]
testpaths = ["tests"]
markers = [
  "e2e: opt-in end-to-end tests using multipass-sdk (requires Multipass)",
]

[tool.ruff]
line-length = 100
target-version = "py310"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B"]

[tool.mypy]
python_version = "3.10"
strict = true
files = ["src"]
```

(mypy checks `src` only; pytest test functions intentionally omit return
annotations, so type-checking `tests` under `strict` would be noise.)

`src/ray_dispatcher/__init__.py`:

```python
"""ray_dispatcher: a generic, Ray-scheduled dispatcher for subprocess jobs on VMs."""

__version__ = "0.1.0"

__all__ = ["__version__"]
```

- [ ] **Step 3: Sync the environment**

Run: `uv sync`
Expected: resolves dependencies and creates `.venv` (Ray, Fabric, PyYAML, dev tools).

- [ ] **Step 4: Run the smoke test to verify it passes**

Run: `uv run pytest tests/unit/test_smoke.py -v`
Expected: `1 passed`.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml README.md .gitignore src/ray_dispatcher/__init__.py tests/unit/test_smoke.py
git commit -m "feat: scaffold ray_dispatcher package (uv, src layout, pytest)"
```

---

### Task 2: Exception hierarchy (`errors.py`)

**Files:**
- Create: `src/ray_dispatcher/errors.py`
- Test: `tests/unit/test_errors.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `DispatcherError`, `ConfigurationError(DispatcherError)`, `ModelValidationError(ConfigurationError)`, `PathValidationError(ConfigurationError)`, `RayRuntimeConflictError(DispatcherError)`, `ProvisioningError(DispatcherError)` with `.report`, `HostInUseError(DispatcherError)`, `NoHealthyHostsError(DispatcherError)`, `BatchExistsError(DispatcherError)`, `BatchFailedError(DispatcherError)` with `.results`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_errors.py`:

```python
import pytest

from ray_dispatcher import errors as e


def test_hierarchy_roots():
    assert issubclass(e.ConfigurationError, e.DispatcherError)
    assert issubclass(e.ModelValidationError, e.ConfigurationError)
    assert issubclass(e.PathValidationError, e.ConfigurationError)
    for cls in (
        e.RayRuntimeConflictError,
        e.ProvisioningError,
        e.HostInUseError,
        e.NoHealthyHostsError,
        e.BatchExistsError,
        e.BatchFailedError,
    ):
        assert issubclass(cls, e.DispatcherError)


def test_provisioning_error_carries_report():
    report = object()
    err = e.ProvisioningError(report)
    assert err.report is report
    assert isinstance(err, e.DispatcherError)


def test_batch_failed_error_carries_results():
    results = [1, 2, 3]
    err = e.BatchFailedError(results)
    assert err.results == results


def test_model_and_path_errors_are_raisable():
    with pytest.raises(e.ModelValidationError):
        raise e.ModelValidationError("bad value")
    with pytest.raises(e.PathValidationError):
        raise e.PathValidationError("bad path")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_errors.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ray_dispatcher.errors'`.

- [ ] **Step 3: Write the implementation**

`src/ray_dispatcher/errors.py`:

```python
"""Public exception hierarchy for ray_dispatcher (spec §9.3)."""

from __future__ import annotations

from typing import Any


class DispatcherError(Exception):
    """Base class for all ray_dispatcher errors.

    Also raised directly for unclassified backend-wide failures.
    """


class ConfigurationError(DispatcherError):
    """Raised synchronously, before submission, for invalid configuration."""


class ModelValidationError(ConfigurationError):
    """Invalid value object (RemoteHost/Inventory/Project/Job/SecretFile/...)."""


class PathValidationError(ConfigurationError):
    """A path is absolute, contains '..', or escapes the run root."""


class RayRuntimeConflictError(DispatcherError):
    """``ray.is_initialized()`` was already true at setup() (spec §3.2)."""


class ProvisioningError(DispatcherError):
    """Setup failed. Carries the ProvisioningReport."""

    def __init__(self, report: Any, message: str | None = None) -> None:
        self.report = report
        super().__init__(message or "provisioning failed")


class HostInUseError(DispatcherError):
    """A remote session lock is held by another live Dispatcher session (§3.2)."""


class NoHealthyHostsError(DispatcherError):
    """No healthy host capacity remains for pending work (§8.2)."""


class BatchExistsError(DispatcherError):
    """The local batch directory already exists (§4.5)."""


class BatchFailedError(DispatcherError):
    """raise_on_failure=True and at least one job failed; carries ordered results."""

    def __init__(self, results: Any, message: str | None = None) -> None:
        self.results = results
        super().__init__(message or "batch completed with failures")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_errors.py -v`
Expected: `4 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/errors.py tests/unit/test_errors.py
git commit -m "feat: add public exception hierarchy"
```

---

### Task 3: Path safety (`paths.py`)

**Files:**
- Create: `src/ray_dispatcher/paths.py`
- Test: `tests/unit/test_paths.py`

**Interfaces:**
- Consumes: `PathValidationError` from `errors.py`.
- Produces:
  - `normalize_relative(path: str, *, field: str = "path") -> str` — static check, returns the normalized POSIX relative path or raises `PathValidationError`.
  - `ensure_within(root: pathlib.Path, relative: str, *, field: str = "path") -> pathlib.Path` — resolves symlinks under `root` and returns the absolute path, or raises `PathValidationError` if it escapes.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_paths.py`:

```python
import os

import pytest

from ray_dispatcher.errors import PathValidationError
from ray_dispatcher.paths import ensure_within, normalize_relative


@pytest.mark.parametrize("good,expected", [
    ("a/b/c", "a/b/c"),
    ("./a/./b", "a/b"),
    (".", "."),
    ("config_files/eval_smoke.json", "config_files/eval_smoke.json"),
])
def test_normalize_relative_accepts(good, expected):
    assert normalize_relative(good) == expected


@pytest.mark.parametrize("bad", [
    "",
    "/etc/passwd",
    "../escape",
    "a/../../b",
    "a/../b",          # any '..' component is rejected, even if it would collapse
    "with\x00nul",
])
def test_normalize_relative_rejects(bad):
    with pytest.raises(PathValidationError):
        normalize_relative(bad)


def test_ensure_within_accepts_nested(tmp_path):
    target = ensure_within(tmp_path, "sub/dir/file.txt")
    assert str(target).startswith(str(tmp_path.resolve()))


def test_ensure_within_rejects_symlink_escape(tmp_path):
    outside = tmp_path.parent / "outside"
    outside.mkdir()
    link = tmp_path / "link"
    os.symlink(outside, link)
    with pytest.raises(PathValidationError):
        ensure_within(tmp_path, "link/secret.txt")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_paths.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ray_dispatcher.paths'`.

- [ ] **Step 3: Write the implementation**

`src/ray_dispatcher/paths.py`:

```python
"""Path normalization and containment checks (spec §4.3, §6, §7)."""

from __future__ import annotations

import posixpath
from pathlib import Path

from .errors import PathValidationError


def normalize_relative(path: str, *, field: str = "path") -> str:
    """Return a normalized, run-root-relative POSIX path or raise.

    Rejects empty, NUL-containing, absolute, and any path with a ``..``
    component. Does not touch the filesystem.
    """
    if not isinstance(path, str) or path == "":
        raise PathValidationError(f"{field} must be a non-empty string")
    if "\x00" in path:
        raise PathValidationError(f"{field} must not contain NUL bytes")
    if path.startswith("/"):
        raise PathValidationError(f"{field} must be relative, got absolute: {path!r}")
    if ".." in path.split("/"):
        raise PathValidationError(f"{field} must not contain '..': {path!r}")
    normalized = posixpath.normpath(path)
    if normalized.startswith("/") or normalized == ".." or normalized.startswith("../"):
        raise PathValidationError(f"{field} escapes the root: {path!r}")
    return normalized


def ensure_within(root: Path, relative: str, *, field: str = "path") -> Path:
    """Resolve ``relative`` beneath ``root`` (following symlinks) and verify
    the resolved path does not escape ``root``. Raises on escape."""
    rel = normalize_relative(relative, field=field)
    root_resolved = root.resolve()
    candidate = (root_resolved / rel).resolve()
    if candidate != root_resolved and root_resolved not in candidate.parents:
        raise PathValidationError(f"{field} escapes root {root}: {relative!r}")
    return candidate
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_paths.py -v`
Expected: all parametrized cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/paths.py tests/unit/test_paths.py
git commit -m "feat: add path normalization and symlink-containment checks"
```

---

### Task 4: Host and inventory models (`models.py` part 1)

**Files:**
- Create: `src/ray_dispatcher/models.py`
- Test: `tests/unit/test_models_hosts.py`

**Interfaces:**
- Consumes: `ModelValidationError` from `errors.py`.
- Produces:
  - `RemoteHost(host, user, slots=1, port=22, identity_file=None, known_hosts_file="~/.ssh/known_hosts")` — frozen.
  - `Inventory(hosts: tuple[RemoteHost, ...])` — frozen; `Inventory.from_yaml(path: str) -> Inventory`.
  - Internal helper `_is_posix_env_name(name: str) -> bool` (used by later tasks).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_models_hosts.py`:

```python
import textwrap

import pytest

from ray_dispatcher.errors import ModelValidationError
from ray_dispatcher.models import Inventory, RemoteHost


def test_remotehost_valid_defaults():
    h = RemoteHost("10.0.0.11", user="ubuntu")
    assert h.slots == 1
    assert h.port == 22
    assert h.identity_file is None
    assert h.known_hosts_file == "~/.ssh/known_hosts"


@pytest.mark.parametrize("kwargs", [
    {"host": "", "user": "ubuntu"},
    {"host": "h", "user": ""},
    {"host": "h", "user": "u", "slots": 0},
    {"host": "h", "user": "u", "slots": -1},
    {"host": "h", "user": "u", "port": 0},
    {"host": "h", "user": "u", "port": 70000},
])
def test_remotehost_rejects(kwargs):
    with pytest.raises(ModelValidationError):
        RemoteHost(**kwargs)


def test_inventory_rejects_empty():
    with pytest.raises(ModelValidationError):
        Inventory(())


def test_inventory_rejects_duplicates():
    a = RemoteHost("h", user="u")
    b = RemoteHost("h", user="u")  # same (host, port, user)
    with pytest.raises(ModelValidationError):
        Inventory((a, b))


def test_inventory_allows_same_host_different_user():
    a = RemoteHost("h", user="u1")
    b = RemoteHost("h", user="u2")
    inv = Inventory((a, b))
    assert len(inv.hosts) == 2


def test_inventory_from_yaml(tmp_path):
    p = tmp_path / "hosts.yaml"
    p.write_text(textwrap.dedent("""
        hosts:
          - host: 10.0.0.11
            user: ubuntu
            slots: 2
          - host: 10.0.0.12
            user: ubuntu
            slots: 4
            port: 2222
    """))
    inv = Inventory.from_yaml(str(p))
    assert [h.host for h in inv.hosts] == ["10.0.0.11", "10.0.0.12"]
    assert inv.hosts[1].slots == 4
    assert inv.hosts[1].port == 2222
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_models_hosts.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ray_dispatcher.models'`.

- [ ] **Step 3: Write the implementation**

`src/ray_dispatcher/models.py`:

```python
"""Validated public value objects (spec §4)."""

from __future__ import annotations

import re
from dataclasses import dataclass

import yaml

from .errors import ModelValidationError

_POSIX_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_posix_env_name(name: str) -> bool:
    return bool(_POSIX_ENV_NAME_RE.match(name))


@dataclass(frozen=True)
class RemoteHost:
    host: str
    user: str
    slots: int = 1
    port: int = 22
    identity_file: str | None = None
    known_hosts_file: str = "~/.ssh/known_hosts"

    def __post_init__(self) -> None:
        if not isinstance(self.host, str) or not self.host:
            raise ModelValidationError("host must be a non-empty string")
        if not isinstance(self.user, str) or not self.user:
            raise ModelValidationError("user must be a non-empty string")
        if self.slots < 1:
            raise ModelValidationError(f"slots must be >= 1, got {self.slots}")
        if not (1 <= self.port <= 65535):
            raise ModelValidationError(f"port must be in 1..65535, got {self.port}")


@dataclass(frozen=True)
class Inventory:
    hosts: tuple[RemoteHost, ...]

    def __post_init__(self) -> None:
        if not self.hosts:
            raise ModelValidationError("inventory must contain at least one host")
        seen: set[tuple[str, int, str]] = set()
        for h in self.hosts:
            key = (h.host, h.port, h.user)
            if key in seen:
                raise ModelValidationError(f"duplicate host entry: {key}")
            seen.add(key)

    @classmethod
    def from_yaml(cls, path: str) -> "Inventory":
        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        if not isinstance(data, dict) or "hosts" not in data:
            raise ModelValidationError("inventory YAML must have a top-level 'hosts' list")
        raw_hosts = data["hosts"]
        if not isinstance(raw_hosts, list):
            raise ModelValidationError("'hosts' must be a list")
        hosts = tuple(RemoteHost(**entry) for entry in raw_hosts)
        return cls(hosts)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_models_hosts.py -v`
Expected: all cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/models.py tests/unit/test_models_hosts.py
git commit -m "feat: add RemoteHost and Inventory value objects"
```

---

### Task 5: Project and secrets models (`models.py` part 2)

**Files:**
- Modify: `src/ray_dispatcher/models.py` (append `SecretFile`, `Project`)
- Test: `tests/unit/test_models_project.py`

**Interfaces:**
- Consumes: `ModelValidationError`, `_is_posix_env_name` (Task 4).
- Produces:
  - `SecretFile(source, remote_name, env_var=None, mode=0o600)` — frozen.
  - `Project(path, project_id, python, uv_version, secrets=(), exclude=(".venv/", ".git/", "solutions/"), dependency_groups=())` — frozen.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_models_project.py`:

```python
import pytest

from ray_dispatcher.errors import ModelValidationError
from ray_dispatcher.models import Project, SecretFile


def test_secretfile_valid():
    s = SecretFile(source="~/lic/gurobi.lic", remote_name="gurobi.lic",
                   env_var="GRB_LICENSE_FILE")
    assert s.mode == 0o600


@pytest.mark.parametrize("kwargs", [
    {"source": "", "remote_name": "x"},
    {"source": "s", "remote_name": ""},
    {"source": "s", "remote_name": "sub/x"},     # not a bare filename
    {"source": "s", "remote_name": ".."},
    {"source": "s", "remote_name": "x", "env_var": "1BAD"},
])
def test_secretfile_rejects(kwargs):
    with pytest.raises(ModelValidationError):
        SecretFile(**kwargs)


def test_project_valid_defaults():
    p = Project(path="../DFaasOptimizer", project_id="dfaas-optimizer",
                python="3.10.18", uv_version="0.11.25")
    assert p.exclude == (".venv/", ".git/", "solutions/")
    assert p.secrets == ()


@pytest.mark.parametrize("field,value", [
    ("python", "3.10"),       # not exact X.Y.Z
    ("python", "3.10.x"),
    ("uv_version", "0.11"),
    ("project_id", "bad id with spaces"),
    ("path", ""),
])
def test_project_rejects(field, value):
    base = dict(path="p", project_id="pid", python="3.10.18", uv_version="0.11.25")
    base[field] = value
    with pytest.raises(ModelValidationError):
        Project(**base)


def test_project_rejects_duplicate_secret_names():
    s1 = SecretFile(source="a", remote_name="dup")
    s2 = SecretFile(source="b", remote_name="dup")
    with pytest.raises(ModelValidationError):
        Project(path="p", project_id="pid", python="3.10.18", uv_version="0.11.25",
                secrets=(s1, s2))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_models_project.py -v`
Expected: FAIL with `ImportError: cannot import name 'Project'`.

- [ ] **Step 3: Write the implementation (append to `models.py`)**

Add near the top, after `_POSIX_ENV_NAME_RE`:

```python
_EXACT_VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")
_PROJECT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
```

Append after `Inventory`:

```python
@dataclass(frozen=True)
class SecretFile:
    source: str
    remote_name: str
    env_var: str | None = None
    mode: int = 0o600

    def __post_init__(self) -> None:
        if not self.source:
            raise ModelValidationError("secret source must be non-empty")
        if not self.remote_name or "/" in self.remote_name or self.remote_name in (".", ".."):
            raise ModelValidationError(
                f"secret remote_name must be a bare filename, got {self.remote_name!r}"
            )
        if self.env_var is not None and not _is_posix_env_name(self.env_var):
            raise ModelValidationError(f"invalid secret env_var: {self.env_var!r}")


@dataclass(frozen=True)
class Project:
    path: str
    project_id: str
    python: str
    uv_version: str
    secrets: tuple[SecretFile, ...] = ()
    exclude: tuple[str, ...] = (".venv/", ".git/", "solutions/")
    dependency_groups: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.path:
            raise ModelValidationError("project path must be non-empty")
        if not _PROJECT_ID_RE.match(self.project_id):
            raise ModelValidationError(f"invalid project_id: {self.project_id!r}")
        if not _EXACT_VERSION_RE.match(self.python):
            raise ModelValidationError(f"python must be exact X.Y.Z, got {self.python!r}")
        if not _EXACT_VERSION_RE.match(self.uv_version):
            raise ModelValidationError(
                f"uv_version must be exact X.Y.Z, got {self.uv_version!r}"
            )
        names = [s.remote_name for s in self.secrets]
        if len(names) != len(set(names)):
            raise ModelValidationError("duplicate secret remote_name in project")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_models_project.py -v`
Expected: all cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/models.py tests/unit/test_models_project.py
git commit -m "feat: add SecretFile and Project value objects"
```

---

### Task 6: Inputs, outputs, and jobs (`models.py` part 3)

**Files:**
- Modify: `src/ray_dispatcher/models.py` (append `InputSpec`, `OutputSpec`, `Job`)
- Test: `tests/unit/test_models_jobs.py`

**Interfaces:**
- Consumes: `ModelValidationError`, `_is_posix_env_name`, and `normalize_relative` from `paths.py`.
- Produces:
  - `InputSpec(source, destination)` — frozen; `destination` stored normalized.
  - `OutputSpec(source, destination=None, required=True)` — frozen; `source`/`destination` stored normalized.
  - `Job(id, command, inputs=(), outputs=(), env={}, timeout_s=None, cwd=".")` — frozen; `cwd` stored normalized.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_models_jobs.py`:

```python
import pytest

from ray_dispatcher.errors import ModelValidationError, PathValidationError
from ray_dispatcher.models import InputSpec, Job, OutputSpec


def test_inputspec_normalizes_destination():
    spec = InputSpec(source="/abs/local/file", destination="./config/./a.json")
    assert spec.destination == "config/a.json"


def test_inputspec_rejects_escaping_destination():
    with pytest.raises(PathValidationError):
        InputSpec(source="s", destination="../escape")


def test_outputspec_defaults_required_true():
    spec = OutputSpec(source="solutions/eval_smoke")
    assert spec.required is True
    assert spec.destination is None


def test_job_valid():
    job = Job(
        id="madea-smoke",
        command=("python", "run.py", "--config", "c.json"),
        inputs=(InputSpec("c.json", "c.json"),),
        outputs=(OutputSpec("solutions/eval_smoke"),),
    )
    assert job.cwd == "."


@pytest.mark.parametrize("bad_id", ["", "-leading", "has space", "a" * 129, "x/y"])
def test_job_rejects_bad_id(bad_id):
    with pytest.raises(ModelValidationError):
        Job(id=bad_id, command=("echo", "hi"))


def test_job_rejects_empty_command():
    with pytest.raises(ModelValidationError):
        Job(id="j", command=())


def test_job_rejects_nul_in_command():
    with pytest.raises(ModelValidationError):
        Job(id="j", command=("echo", "a\x00b"))


def test_job_rejects_bad_env_key():
    with pytest.raises(ModelValidationError):
        Job(id="j", command=("echo",), env={"1BAD": "x"})


def test_job_rejects_nonpositive_timeout():
    with pytest.raises(ModelValidationError):
        Job(id="j", command=("echo",), timeout_s=0)


def test_job_rejects_escaping_cwd():
    with pytest.raises(PathValidationError):
        Job(id="j", command=("echo",), cwd="../escape")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_models_jobs.py -v`
Expected: FAIL with `ImportError: cannot import name 'Job'`.

- [ ] **Step 3: Write the implementation (append to `models.py`)**

Update the imports block of `models.py`: change `from dataclasses import dataclass`
to `from dataclasses import dataclass, field`, and add the two imports below. The
final stdlib/first-party ordering does not matter yet — Task 8 runs
`ruff check --fix` to sort it.

```python
from collections.abc import Mapping
from dataclasses import dataclass, field   # field is newly added
from .paths import normalize_relative
```

Add near the other module-level regexes:

```python
_JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
```

Append after `Project`:

```python
@dataclass(frozen=True)
class InputSpec:
    source: str                       # absolute or Project.path-relative local path
    destination: str                  # normalized run-root-relative POSIX path

    def __post_init__(self) -> None:
        if not self.source:
            raise ModelValidationError("input source must be non-empty")
        object.__setattr__(
            self, "destination", normalize_relative(self.destination, field="input destination")
        )


@dataclass(frozen=True)
class OutputSpec:
    source: str                       # normalized run-root-relative POSIX path
    destination: str | None = None    # relative to the job's local outputs dir
    required: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "source", normalize_relative(self.source, field="output source")
        )
        if self.destination is not None:
            object.__setattr__(
                self,
                "destination",
                normalize_relative(self.destination, field="output destination"),
            )


@dataclass(frozen=True)
class Job:
    id: str
    command: tuple[str, ...]
    inputs: tuple[InputSpec, ...] = ()
    outputs: tuple[OutputSpec, ...] = ()
    env: Mapping[str, str] = field(default_factory=dict)
    timeout_s: float | None = None
    cwd: str = "."

    def __post_init__(self) -> None:
        if not _JOB_ID_RE.match(self.id):
            raise ModelValidationError(f"invalid job id: {self.id!r}")
        if not self.command:
            raise ModelValidationError("command must be non-empty")
        for part in self.command:
            if "\x00" in part:
                raise ModelValidationError("command parts must not contain NUL bytes")
        for key in self.env:
            if not _is_posix_env_name(key):
                raise ModelValidationError(f"invalid env key: {key!r}")
        if self.timeout_s is not None and self.timeout_s <= 0:
            raise ModelValidationError("timeout_s must be > 0 when set")
        object.__setattr__(self, "cwd", normalize_relative(self.cwd, field="cwd"))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_models_jobs.py -v`
Expected: all cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/models.py tests/unit/test_models_jobs.py
git commit -m "feat: add InputSpec, OutputSpec, and Job value objects"
```

---

### Task 7: Status, handles, attempts, and results (`models.py` part 4)

**Files:**
- Modify: `src/ray_dispatcher/models.py` (append enums + result value objects)
- Test: `tests/unit/test_models_results.py`

**Interfaces:**
- Consumes: `ModelValidationError`.
- Produces: `JobStatus`, `FailureKind` (enums); `RetryPolicy(max_attempts=2, retry_on=frozenset({SSH, HOST_LOST, COLLECTION}))`; `JobHandle(batch_id, job_id, token)`; `AttemptResult(...)`; `JobResult(...)`; `HostProvisioningResult(...)`; `ProvisioningReport(hosts)`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_models_results.py`:

```python
import pytest

from ray_dispatcher.errors import ModelValidationError
from ray_dispatcher.models import (
    AttemptResult,
    FailureKind,
    HostProvisioningResult,
    JobHandle,
    JobResult,
    JobStatus,
    ProvisioningReport,
    RetryPolicy,
)


def test_job_status_values():
    assert {s.value for s in JobStatus} == {
        "pending", "running", "succeeded", "failed", "timed_out", "cancelled",
    }


def test_failure_kind_values():
    assert {k.value for k in FailureKind} == {
        "command", "ssh", "timeout", "output_missing", "collection", "host_lost",
        "internal",
    }


def test_retry_policy_defaults():
    policy = RetryPolicy()
    assert policy.max_attempts == 2
    assert policy.retry_on == frozenset(
        {FailureKind.SSH, FailureKind.HOST_LOST, FailureKind.COLLECTION}
    )


def test_retry_policy_rejects_zero_attempts():
    with pytest.raises(ModelValidationError):
        RetryPolicy(max_attempts=0)


def test_job_handle_is_hashable():
    h = JobHandle(batch_id="b", job_id="j", token="t")
    assert h in {h}


def test_result_objects_construct():
    attempt = AttemptResult(
        number=1, host="h", status=JobStatus.SUCCEEDED, returncode=0,
        duration_s=1.0, stdout_log="o", stderr_log="e",
    )
    result = JobResult(
        id="j", batch_id="b", status=JobStatus.SUCCEEDED, returncode=0,
        duration_s=1.0, host="h", output_dir="./results/b/j/outputs",
        attempts=(attempt,),
    )
    assert result.attempts[0].failure_kind is None
    report = ProvisioningReport(
        hosts=(HostProvisioningResult(host="h", succeeded=True,
                                      source_digest="s", environment_digest="e"),)
    )
    assert report.hosts[0].succeeded is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_models_results.py -v`
Expected: FAIL with `ImportError: cannot import name 'JobStatus'`.

- [ ] **Step 3: Write the implementation (append to `models.py`)**

Add to the imports block of `models.py`:

```python
from enum import Enum
```

Append at the end of `models.py`:

```python
class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"


class FailureKind(Enum):
    COMMAND = "command"
    SSH = "ssh"
    TIMEOUT = "timeout"
    OUTPUT_MISSING = "output_missing"
    COLLECTION = "collection"
    HOST_LOST = "host_lost"
    INTERNAL = "internal"


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 2
    retry_on: frozenset[FailureKind] = frozenset(
        {FailureKind.SSH, FailureKind.HOST_LOST, FailureKind.COLLECTION}
    )

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ModelValidationError("max_attempts must be >= 1")


@dataclass(frozen=True)
class JobHandle:
    batch_id: str
    job_id: str
    token: str  # opaque; consumers must not interpret it


@dataclass(frozen=True)
class AttemptResult:
    number: int
    host: str
    status: JobStatus
    returncode: int | None
    duration_s: float
    stdout_log: str
    stderr_log: str
    failure_kind: FailureKind | None = None
    error: str | None = None


@dataclass(frozen=True)
class JobResult:
    id: str
    batch_id: str
    status: JobStatus
    returncode: int | None
    duration_s: float
    host: str | None
    output_dir: str | None
    attempts: tuple[AttemptResult, ...]
    error: str | None = None


@dataclass(frozen=True)
class HostProvisioningResult:
    host: str
    succeeded: bool
    source_digest: str | None
    environment_digest: str | None
    error: str | None = None


@dataclass(frozen=True)
class ProvisioningReport:
    hosts: tuple[HostProvisioningResult, ...]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_models_results.py -v`
Expected: all cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/models.py tests/unit/test_models_results.py
git commit -m "feat: add status, handle, attempt, and result value objects"
```

---

### Task 8: Public exports + full-suite green

**Files:**
- Modify: `src/ray_dispatcher/__init__.py`
- Test: `tests/unit/test_public_api.py`

**Interfaces:**
- Consumes: all of `models.py` and `errors.py`.
- Produces: top-level imports `from ray_dispatcher import Dispatcher`-style for the value objects and errors that exist in Phase 1 (the `Dispatcher`, `ExecutionBackend`, and `SecretFile` execution come in later phases; only what exists is exported now).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_public_api.py`:

```python
import ray_dispatcher as rd


def test_public_value_objects_importable():
    for name in [
        "RemoteHost", "Inventory", "SecretFile", "Project",
        "InputSpec", "OutputSpec", "Job",
        "JobStatus", "FailureKind", "RetryPolicy", "JobHandle",
        "AttemptResult", "JobResult", "HostProvisioningResult", "ProvisioningReport",
    ]:
        assert hasattr(rd, name), name


def test_public_errors_importable():
    for name in [
        "DispatcherError", "ConfigurationError", "ModelValidationError",
        "PathValidationError", "RayRuntimeConflictError", "ProvisioningError",
        "HostInUseError", "NoHealthyHostsError", "BatchExistsError",
        "BatchFailedError",
    ]:
        assert hasattr(rd, name), name


def test_all_names_resolve():
    for name in rd.__all__:
        assert hasattr(rd, name), name
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_public_api.py -v`
Expected: FAIL (`RemoteHost` not an attribute of `ray_dispatcher`).

- [ ] **Step 3: Write the implementation**

Replace `src/ray_dispatcher/__init__.py` with:

```python
"""ray_dispatcher: a generic, Ray-scheduled dispatcher for subprocess jobs on VMs."""

from .errors import (
    BatchExistsError,
    BatchFailedError,
    ConfigurationError,
    DispatcherError,
    HostInUseError,
    ModelValidationError,
    NoHealthyHostsError,
    PathValidationError,
    ProvisioningError,
    RayRuntimeConflictError,
)
from .models import (
    AttemptResult,
    FailureKind,
    HostProvisioningResult,
    InputSpec,
    Inventory,
    Job,
    JobHandle,
    JobResult,
    JobStatus,
    OutputSpec,
    Project,
    ProvisioningReport,
    RemoteHost,
    RetryPolicy,
    SecretFile,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    # value objects
    "RemoteHost",
    "Inventory",
    "SecretFile",
    "Project",
    "InputSpec",
    "OutputSpec",
    "Job",
    "JobStatus",
    "FailureKind",
    "RetryPolicy",
    "JobHandle",
    "AttemptResult",
    "JobResult",
    "HostProvisioningResult",
    "ProvisioningReport",
    # errors
    "DispatcherError",
    "ConfigurationError",
    "ModelValidationError",
    "PathValidationError",
    "RayRuntimeConflictError",
    "ProvisioningError",
    "HostInUseError",
    "NoHealthyHostsError",
    "BatchExistsError",
    "BatchFailedError",
]
```

- [ ] **Step 4: Run the full suite + linters to verify everything passes**

Run: `uv run pytest -q`
Expected: all tests `passed`.

Run: `uv run ruff check --fix . && uv run ruff check . && uv run mypy`
Expected: the first command auto-sorts imports and exits clean; the second
reports `All checks passed!`; mypy reports `Success: no issues found`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/__init__.py tests/unit/test_public_api.py
git commit -m "feat: export public value objects and errors"
```

---

## Phase 1 self-review

Run before declaring Phase 1 done:

- [ ] Every spec §4 value object exists and is exported: `RemoteHost`, `Inventory`, `SecretFile`, `Project`, `InputSpec`, `OutputSpec`, `Job`, `JobStatus`, `FailureKind`, `RetryPolicy`, `JobHandle`, `AttemptResult`, `JobResult`, `HostProvisioningResult`, `ProvisioningReport`.
- [ ] Every §9.3 exception exists and is exported.
- [ ] Validation rules from §4.1–§4.4 each have a rejecting test.
- [ ] `normalize_relative` rejects absolute, `..`, NUL, empty; `ensure_within` rejects symlink escape.
- [ ] `uv run pytest -q`, `uv run ruff check .`, and `uv run mypy` are all green.

**Deliverable:** a tested, lint-clean, type-clean value/validation layer that Phases 2–6 import. No VM, Ray, or network behaviour yet.
