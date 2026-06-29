# Phase 5a — Results Tree (layout, collection, publish, manifests) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `src/ray_dispatcher/results.py` — the local result tree for one job: path layout (§9.1), best-effort output collection with required/optional classification (§7.8), atomic publish of a successful attempt's outputs (§7.9), and JSON manifests for each attempt and the job result.

**Architecture:** Pure host-local filesystem logic plus downloads through the existing `Transport.pull`. No Ray, no remote setup. `JobLayout` computes the `<results_dir>/<batch_id>/<job_id>/…` paths; `collect_outputs` pulls declared outputs into attempt-scoped staging and reports what landed; `publish_job_outputs` renames staging into the job's final `outputs/`; `write_attempt_json`/`write_result_json` serialize the frozen dataclasses already defined in `models.py`. The Phase 5b attempt driver and the Phase 6 backend call these helpers.

**Tech Stack:** Python 3.10+, stdlib `json`/`pathlib`/`dataclasses`/`enum`, the existing `ray_dispatcher.ssh.Transport` and `ray_dispatcher.paths.ensure_within`. Tests use `pytest` with `tmp_path` and `ray_dispatcher.ssh.FakeTransport`.

## Global Constraints

- Python floor 3.10; `from __future__ import annotations` at the top of every module (matches existing files).
- mypy strict (`files=["src"]`) and ruff (`E/F/I/UP/B`, line-length 100) must pass with zero errors.
- TDD: every behavior gets a failing test first. No new third-party dependency — stdlib only here.
- Frozen dataclasses for value objects (`CollectionResult`), matching the `models.py` style.
- All data types consumed are already in `models.py` and must NOT be redefined: `AttemptResult`, `JobResult`, `OutputSpec`, `JobStatus`, `FailureKind`.
- Local result layout is fixed by spec §9.1: `<results_dir>/<batch_id>/<job_id>/{attempts/<n>/{stdout.log,stderr.log,attempt.json}, outputs/, result.json}`.
- Attempts never overwrite one another (§9.1); final outputs come from exactly one successful attempt (§7.9 — atomic publish).
- Output destinations must remain beneath the local job output directory; reject absolute / `..` / symlink-escaping paths via `paths.ensure_within` (§4.3).
- Serialization rule: enums serialize as their `.value`; anything not natively JSON-serializable and not an enum is an error (raise `TypeError`) — never silently stringify.

---

### Task 1: `JobLayout` + `create_attempt_dir`

**Files:**
- Create: `src/ray_dispatcher/results.py`
- Test: `tests/unit/test_results_layout.py`

**Interfaces:**
- Consumes: nothing from earlier Phase 5 tasks (first task).
- Produces:
  - `JobLayout(results_dir: str, batch_id: str, job_id: str)` with attribute `job_dir: Path` and properties `attempts_dir: Path`, `outputs_dir: Path`, `result_json: Path`; methods `attempt_dir(n: int) -> Path`, `stdout_log(n: int) -> Path`, `stderr_log(n: int) -> Path`, `attempt_json(n: int) -> Path`.
  - `create_attempt_dir(layout: JobLayout, n: int) -> Path` — creates `attempts/<n>` (parents as needed) and returns it; raises `FileExistsError` if that attempt dir already exists (attempts are never reused, §9.1).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_results_layout.py`:

```python
import pytest

from ray_dispatcher.results import JobLayout, create_attempt_dir


def _layout(tmp_path):
    return JobLayout(str(tmp_path / "results"), "batch1", "jobA")


def test_layout_paths_match_spec_9_1(tmp_path):
    lo = _layout(tmp_path)
    base = tmp_path / "results" / "batch1" / "jobA"
    assert lo.job_dir == base
    assert lo.attempts_dir == base / "attempts"
    assert lo.outputs_dir == base / "outputs"
    assert lo.result_json == base / "result.json"
    assert lo.attempt_dir(1) == base / "attempts" / "1"
    assert lo.stdout_log(2) == base / "attempts" / "2" / "stdout.log"
    assert lo.stderr_log(2) == base / "attempts" / "2" / "stderr.log"
    assert lo.attempt_json(3) == base / "attempts" / "3" / "attempt.json"


def test_create_attempt_dir_makes_dir(tmp_path):
    lo = _layout(tmp_path)
    d = create_attempt_dir(lo, 1)
    assert d.is_dir()
    assert d == lo.attempt_dir(1)


def test_create_attempt_dir_rejects_reuse(tmp_path):
    lo = _layout(tmp_path)
    create_attempt_dir(lo, 1)
    with pytest.raises(FileExistsError):
        create_attempt_dir(lo, 1)  # attempts are never reused (§9.1)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_results_layout.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ray_dispatcher.results'`.

- [ ] **Step 3: Write the implementation**

Create `src/ray_dispatcher/results.py`:

```python
"""Local result tree: layout, output collection, atomic publish, manifests (spec §9.1).

All paths here are local to the dispatcher host. The Phase 5b attempt driver and
the Phase 6 backend call collect_outputs/publish_job_outputs and the manifest
writers; nothing in this module touches Ray.
"""

from __future__ import annotations

from pathlib import Path


class JobLayout:
    """Local result paths for one job: <results_dir>/<batch_id>/<job_id>/ (spec §9.1)."""

    def __init__(self, results_dir: str, batch_id: str, job_id: str) -> None:
        self.job_dir = Path(results_dir) / batch_id / job_id

    @property
    def attempts_dir(self) -> Path:
        return self.job_dir / "attempts"

    @property
    def outputs_dir(self) -> Path:
        return self.job_dir / "outputs"

    @property
    def result_json(self) -> Path:
        return self.job_dir / "result.json"

    def attempt_dir(self, n: int) -> Path:
        return self.attempts_dir / str(n)

    def stdout_log(self, n: int) -> Path:
        return self.attempt_dir(n) / "stdout.log"

    def stderr_log(self, n: int) -> Path:
        return self.attempt_dir(n) / "stderr.log"

    def attempt_json(self, n: int) -> Path:
        return self.attempt_dir(n) / "attempt.json"


def create_attempt_dir(layout: JobLayout, n: int) -> Path:
    """Create attempts/<n>; reusing an attempt number is a bug (spec §9.1)."""
    d = layout.attempt_dir(n)
    d.mkdir(parents=True)  # exist_ok=False -> FileExistsError if the attempt dir exists
    return d
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_results_layout.py -v`
Expected: all three cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/results.py tests/unit/test_results_layout.py
git commit -m "feat: add results JobLayout + create_attempt_dir (spec §9.1)"
```

---

### Task 2: `write_attempt_json` + `write_result_json`

**Files:**
- Modify: `src/ray_dispatcher/results.py` (append serializers + the `_enc` helper)
- Test: `tests/unit/test_results_manifest.py`

**Interfaces:**
- Consumes: `models.AttemptResult`, `models.JobResult`, `models.JobStatus`, `models.FailureKind`.
- Produces:
  - `write_attempt_json(path: Path, attempt: AttemptResult, *, missing_optional: tuple[str, ...] = ()) -> None` — writes `attempt.json`; serializes all `AttemptResult` fields plus a `"missing_optional"` list (§7.8 records missing optional outputs in the attempt manifest).
  - `write_result_json(path: Path, result: JobResult) -> None` — writes `result.json` (the `JobResult`, including its nested `attempts`).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_results_manifest.py`:

```python
import json

from ray_dispatcher.models import AttemptResult, FailureKind, JobResult, JobStatus
from ray_dispatcher.results import write_attempt_json, write_result_json


def _attempt(n=1, status=JobStatus.SUCCEEDED, fk=None):
    return AttemptResult(
        number=n,
        host="10.0.0.1",
        status=status,
        returncode=0 if status is JobStatus.SUCCEEDED else 1,
        duration_s=1.5,
        stdout_log="attempts/1/stdout.log",
        stderr_log="attempts/1/stderr.log",
        failure_kind=fk,
        error=None,
    )


def test_write_attempt_json_serializes_enums_as_values(tmp_path):
    p = tmp_path / "attempt.json"
    write_attempt_json(p, _attempt(status=JobStatus.FAILED, fk=FailureKind.COMMAND),
                       missing_optional=("logs/extra.txt",))
    doc = json.loads(p.read_text())
    assert doc["number"] == 1
    assert doc["status"] == "failed"               # enum -> .value
    assert doc["failure_kind"] == "command"        # enum -> .value
    assert doc["returncode"] == 1
    assert doc["missing_optional"] == ["logs/extra.txt"]


def test_write_attempt_json_null_failure_kind(tmp_path):
    p = tmp_path / "attempt.json"
    write_attempt_json(p, _attempt())              # success: no failure_kind
    doc = json.loads(p.read_text())
    assert doc["status"] == "succeeded"
    assert doc["failure_kind"] is None
    assert doc["missing_optional"] == []           # default empty


def test_write_result_json_includes_nested_attempts(tmp_path):
    p = tmp_path / "result.json"
    result = JobResult(
        id="jobA",
        batch_id="batch1",
        status=JobStatus.SUCCEEDED,
        returncode=0,
        duration_s=2.0,
        host="10.0.0.1",
        output_dir="results/batch1/jobA/outputs",
        attempts=(_attempt(),),
        error=None,
    )
    write_result_json(p, result)
    doc = json.loads(p.read_text())
    assert doc["id"] == "jobA"
    assert doc["status"] == "succeeded"
    assert isinstance(doc["attempts"], list) and len(doc["attempts"]) == 1
    assert doc["attempts"][0]["status"] == "succeeded"
    assert doc["output_dir"].endswith("/outputs")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_results_manifest.py -v`
Expected: FAIL with `ImportError: cannot import name 'write_attempt_json'`.

- [ ] **Step 3: Write the implementation**

Add to the imports block of `results.py`:

```python
import json
from dataclasses import asdict
from enum import Enum
```

(`from pathlib import Path` is already present from Task 1.) Add these imports for the model types:

```python
from .models import AttemptResult, JobResult
```

Append to `results.py`:

```python
def _enc(o: object) -> object:
    """json default: enums serialize as their value; nothing else is allowed."""
    if isinstance(o, Enum):
        return o.value
    raise TypeError(f"not JSON serializable: {type(o).__name__}")


def write_attempt_json(
    path: Path, attempt: AttemptResult, *, missing_optional: tuple[str, ...] = ()
) -> None:
    """Write attempts/<n>/attempt.json (spec §9.1); record missing optional outputs (§7.8)."""
    doc = asdict(attempt)
    doc["missing_optional"] = list(missing_optional)
    path.write_text(json.dumps(doc, default=_enc, indent=2))


def write_result_json(path: Path, result: JobResult) -> None:
    """Write the job's result.json (spec §9.1), including its nested attempts."""
    path.write_text(json.dumps(asdict(result), default=_enc, indent=2))
```

Note: `asdict` recurses into the nested `AttemptResult` tuple (→ list of dicts) and leaves `Enum` members as-is; `default=_enc` then converts them. `None` values pass through natively.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_results_manifest.py -v`
Expected: all three cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/results.py tests/unit/test_results_manifest.py
git commit -m "feat: add attempt/result JSON manifest writers (spec §9.1)"
```

---

### Task 3: `collect_outputs` (+ `CollectionResult`)

**Files:**
- Modify: `src/ray_dispatcher/results.py` (append `CollectionResult` + `collect_outputs`)
- Test: `tests/unit/test_results_collect.py`

**Interfaces:**
- Consumes: `ssh.Transport` (its `pull(remote, local, *, delete=False, excludes=())`), `models.OutputSpec`, `paths.ensure_within(root: Path, relative: str, *, field=...) -> Path`.
- Produces:
  - `CollectionResult` frozen dataclass: `present: tuple[str, ...]`, `missing_required: tuple[str, ...]`, `missing_optional: tuple[str, ...]`.
  - `collect_outputs(transport: Transport, remote_run_dir: str, outputs: tuple[OutputSpec, ...], staging_dir: Path) -> CollectionResult` — for each output: resolves the local destination (`spec.destination or spec.source`) contained beneath `staging_dir`, creates its parent, pulls `f"{remote_run_dir}/{spec.source}"` into it, then classifies by local existence. Missing required → `missing_required` (drives `OUTPUT_MISSING` in the caller); missing optional → `missing_optional`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_results_collect.py`:

```python
import pytest

from ray_dispatcher.errors import PathValidationError
from ray_dispatcher.models import OutputSpec
from ray_dispatcher.results import collect_outputs
from ray_dispatcher.ssh import FakeTransport


def _pulls(t):
    return [c[1] for c in t.calls if c[0] == "pull"]


def test_collect_issues_pull_per_output_with_contained_dest(tmp_path):
    staging = tmp_path / "staging"
    staging.mkdir()
    t = FakeTransport()  # pull is a no-op (records the call); files do not appear
    outputs = (
        OutputSpec(source="solutions/a.json", required=True),
        OutputSpec(source="logs/run.log", destination="run.log", required=False),
    )
    collect_outputs(t, "/home/u/.ray_dispatcher/runs/b/j/1", outputs, staging)
    pulls = _pulls(t)
    # remote source is run-dir-relative; local dest is contained under staging.
    # ensure_within returns a resolved path, so compare against staging.resolve()
    # (pytest's tmp_path can sit under a macOS /var -> /private/var symlink).
    root = staging.resolve()
    assert pulls[0][0] == "/home/u/.ray_dispatcher/runs/b/j/1/solutions/a.json"
    assert pulls[0][1] == str(root / "solutions" / "a.json")
    assert pulls[1][0] == "/home/u/.ray_dispatcher/runs/b/j/1/logs/run.log"
    assert pulls[1][1] == str(root / "run.log")  # destination override


def test_collect_classifies_missing_required_and_optional(tmp_path):
    staging = tmp_path / "staging"
    staging.mkdir()
    # FakeTransport.pull does not create files; simulate a successful pull for
    # 'present.txt' by pre-creating its local destination.
    (staging / "present.txt").write_text("ok")
    t = FakeTransport()
    outputs = (
        OutputSpec(source="present.txt", required=True),
        OutputSpec(source="missing_req.txt", required=True),
        OutputSpec(source="missing_opt.txt", required=False),
    )
    res = collect_outputs(t, "/runs/b/j/1", outputs, staging)
    assert res.present == ("present.txt",)
    assert res.missing_required == ("missing_req.txt",)
    assert res.missing_optional == ("missing_opt.txt",)


def test_collect_rejects_destination_escaping_staging(tmp_path):
    staging = tmp_path / "staging"
    staging.mkdir()
    t = FakeTransport()
    bad = (OutputSpec(source="ok.txt", destination="../escape.txt", required=True),)
    with pytest.raises(PathValidationError):
        collect_outputs(t, "/runs/b/j/1", bad, staging)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_results_collect.py -v`
Expected: FAIL with `ImportError: cannot import name 'collect_outputs'`.

- [ ] **Step 3: Write the implementation**

Add `dataclass` to the existing dataclasses import and add the new imports to `results.py`:

```python
from dataclasses import asdict, dataclass
```

(replacing the `from dataclasses import asdict` line from Task 2), and:

```python
from .models import AttemptResult, JobResult, OutputSpec
from .paths import ensure_within
from .ssh import Transport
```

(merge `OutputSpec` into the existing `from .models import …` line; do not add a second models import.)

Append to `results.py`:

```python
@dataclass(frozen=True)
class CollectionResult:
    present: tuple[str, ...]
    missing_required: tuple[str, ...]
    missing_optional: tuple[str, ...]


def collect_outputs(
    transport: Transport,
    remote_run_dir: str,
    outputs: tuple[OutputSpec, ...],
    staging_dir: Path,
) -> CollectionResult:
    """Best-effort pull of declared outputs into attempt staging (spec §7.8).

    Each output's local destination is contained beneath staging_dir. After the
    pull, an output absent locally is classified: a missing required output
    drives OUTPUT_MISSING in the caller; a missing optional one is recorded.
    """
    present: list[str] = []
    missing_required: list[str] = []
    missing_optional: list[str] = []
    for spec in outputs:
        rel = spec.destination or spec.source
        dest = ensure_within(staging_dir, rel, field="output destination")
        dest.parent.mkdir(parents=True, exist_ok=True)
        transport.pull(f"{remote_run_dir}/{spec.source}", str(dest))
        if dest.exists():
            present.append(rel)
        elif spec.required:
            missing_required.append(rel)
        else:
            missing_optional.append(rel)
    return CollectionResult(tuple(present), tuple(missing_required), tuple(missing_optional))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_results_collect.py -v`
Expected: all three cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/results.py tests/unit/test_results_collect.py
git commit -m "feat: add collect_outputs with required/optional classification (spec §7.8)"
```

---

### Task 4: `publish_job_outputs`

**Files:**
- Modify: `src/ray_dispatcher/results.py` (append `publish_job_outputs`)
- Test: `tests/unit/test_results_publish.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `publish_job_outputs(staging_dir: Path, outputs_dir: Path) -> None` — atomically renames the attempt's collected staging dir into the job's final `outputs/` (spec §7.9). Exactly one successful attempt publishes; the rename fails if `outputs_dir` already exists with content.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_results_publish.py`:

```python
import pytest

from ray_dispatcher.results import publish_job_outputs


def test_publish_renames_staging_into_outputs(tmp_path):
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "a.json").write_text("payload")
    outputs = tmp_path / "job" / "outputs"   # parent 'job' does not exist yet
    publish_job_outputs(staging, outputs)
    assert outputs.is_dir()
    assert (outputs / "a.json").read_text() == "payload"
    assert not staging.exists()              # moved, not copied


def test_publish_refuses_to_clobber_existing_nonempty_outputs(tmp_path):
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "new.json").write_text("new")
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    (outputs / "old.json").write_text("old")  # a prior success already there
    with pytest.raises(OSError):
        publish_job_outputs(staging, outputs)
    assert (outputs / "old.json").read_text() == "old"  # untouched
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_results_publish.py -v`
Expected: FAIL with `ImportError: cannot import name 'publish_job_outputs'`.

- [ ] **Step 3: Write the implementation**

Append to `results.py`:

```python
def publish_job_outputs(staging_dir: Path, outputs_dir: Path) -> None:
    """Atomically publish a successful attempt's collected outputs (spec §7.9).

    One success per job: renaming a non-empty staging dir onto an existing
    non-empty outputs dir raises OSError, so a prior success is never clobbered.
    """
    outputs_dir.parent.mkdir(parents=True, exist_ok=True)
    staging_dir.rename(outputs_dir)  # atomic on the same filesystem
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_results_publish.py -v`
Expected: both cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/results.py tests/unit/test_results_publish.py
git commit -m "feat: add publish_job_outputs atomic rename (spec §7.9)"
```

---

### Task 5: Phase 5a gate

**Files:**
- (no new source) — final full-toolchain verification.

**Interfaces:** none.

- [ ] **Step 1: Run the full suite**

Run: `uv run pytest -q`
Expected: all tests `passed` (Phases 1–4b + the new Phase 5a results tests).

- [ ] **Step 2: Lint + type check**

Run: `uv run ruff check --fix . && uv run ruff check . && uv run mypy`
Expected: ruff auto-sorts then reports `All checks passed!`; mypy reports `Success: no issues found`.

If mypy flags the `asdict`/`default=_enc` path or `Path` return types, fix minimally (no config relaxation). Confirm `results.py` imports `json`/`pathlib`/`dataclasses`/`enum`/`models`/`paths`/`ssh` and NOT `ray`.

- [ ] **Step 3: Commit (only if Step 2 made changes)**

```bash
git add -A
git commit -m "chore: phase 5a gate green (ruff + mypy)"
```

---

## Phase 5a self-review

Run before declaring Phase 5a done:

- [ ] `JobLayout` paths exactly match spec §9.1 (`attempts/<n>/{stdout.log,stderr.log,attempt.json}`, `outputs/`, `result.json`).
- [ ] `create_attempt_dir` raises on reuse (attempts never overwrite, §9.1).
- [ ] `write_attempt_json`/`write_result_json` serialize enums as `.value`, nest attempts, pass `None` through, and record `missing_optional` (§7.8); `_enc` raises `TypeError` for any non-enum unknown type (no silent stringify).
- [ ] `collect_outputs` contains every destination beneath `staging_dir` via `ensure_within` (rejects `..`/absolute/symlink escape, §4.3), pulls `remote_run_dir/<source>`, and classifies missing required vs optional (§7.8).
- [ ] `publish_job_outputs` is an atomic rename and refuses to clobber an existing non-empty `outputs/` (one success per job, §7.9).
- [ ] `uv run pytest -q`, `uv run ruff check .`, and `uv run mypy` all green; `results.py` does not import `ray`.

**Deliverable:** `results.py` — `JobLayout`, `create_attempt_dir`, `write_attempt_json`, `write_result_json`, `CollectionResult`, `collect_outputs`, `publish_job_outputs`. The local result tree the attempt driver writes into and the backend reads from.

**Residuals carried to later phases (documented):**
- **Phase 5b (attempt driver):** `execute_attempt` in `scheduling.py` wires `Transport` + `RemoteLayout.run_dir(batch,job,attempt)` (new helper) + a runner-manifest builder + `remote_runner` invocation + these `results.py` helpers, producing an `AttemptResult`. It supplies `remote_run_dir` to `collect_outputs` and decides `failure_kind` (e.g. `OUTPUT_MISSING` when `missing_required` is non-empty and the command otherwise succeeded, §7.8) and calls `publish_job_outputs` only on success.
- **Phase 6 (backend/dispatcher):** `backends/base.py` `ExecutionBackend` ABC lands here, beside its first implementation (`ssh_ray`) and its consumer (`dispatcher`), so the interface is introduced with a real implementation rather than speculatively. The retry loop (§8.3) classifies `AttemptResult.failure_kind` against `RetryPolicy`; `result.json`/`outputs/` are written by the backend after the final attempt.
