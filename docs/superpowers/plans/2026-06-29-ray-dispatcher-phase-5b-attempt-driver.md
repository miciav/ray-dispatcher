# Phase 5b — Host-Side Attempt Driver (`execute_attempt`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the host-side single-attempt driver `execute_attempt` (spec §7 steps 2–9) that runs one job attempt on one provisioned host over SSH and returns an `AttemptResult`, wiring the existing `Transport`, the remote `remote_runner.py`, and the Phase 5a `results.py` helpers.

**Architecture:** `execute_attempt` is the Ray-free body of the Ray task (spec §5 places the Ray task in `scheduling.py`); Phase 6 wraps it with lease acquisition, retry, and `ray.remote`. It creates a fresh remote run dir, copies the provisioned source on the VM, symlinks the immutable `.venv`, pushes inputs, writes a JSON runner manifest, invokes the versioned runner, parses its `result.json`, pulls logs, collects outputs, classifies the outcome (`COMMAND` / `OUTPUT_MISSING` / success), and publishes a successful attempt's outputs. Every remote command embeds only library-controlled values; all data is `shlex`-quoted or written via `printf %s` (§7).

**Tech Stack:** Python 3.10+, stdlib `json`/`os`/`posixpath`/`shlex`/`pathlib`; existing `ray_dispatcher.ssh` (`Transport`, `TransportError`, `FakeTransport`), `ray_dispatcher.provisioning` (`RemoteLayout`), `ray_dispatcher.results` (Phase 5a), `ray_dispatcher.models`, `ray_dispatcher.errors`. Tests use `pytest` + `tmp_path` + `FakeTransport`.

## Global Constraints

- Python floor 3.10; `from __future__ import annotations` at the top of every module.
- mypy strict (`files=["src"]`) and ruff (`E/F/I/UP/B`, line-length 100) must pass with zero errors. Stdlib-only — no new third-party dependency.
- **§7 no-shell-from-user-strings (binding, security):** every remote command embeds only library-controlled values. Job data (`command`, `env`, `cwd`, input/output paths, secret env values) is NEVER interpolated into a shell command — it travels inside the JSON runner manifest written with `printf %s`. All paths interpolated into `sh -c` strings are `shlex.quote`d. The job argv is executed only by `remote_runner.py` via `subprocess.Popen(argv, ...)` (no shell).
- Remote paths are absolute (resolved under `<home>/.ray_dispatcher` by `RemoteLayout`); rsync `--protect-args`/`shlex` never expand `~`.
- The remote runner manifest keys are fixed by `remote_runner.py` and must match exactly: `argv`, `cwd`, `env`, `secret_env`, `venv_bin`, `virtual_env`, `pid_path`, `stdout_path`, `stderr_path`, `result_path`.
- Attempt protocol order is spec §7: (2) fresh run dir, existing is an error; (3) copy source on VM via rsync + symlink `.venv` to the immutable env; (4) push inputs to explicit destinations; (5) write manifest; (6) invoke runner; (7) host keeps streamed logs in the attempt dir; (8) collect declared outputs, missing required → `OUTPUT_MISSING` only when the command otherwise succeeded; (9) atomically publish a successful attempt's outputs.
- Result durations are the runner's monotonic elapsed time (`result.json["duration_s"]`).
- Data types already in `models.py` (do NOT redefine): `Job`, `InputSpec`, `OutputSpec`, `AttemptResult`, `JobStatus`, `FailureKind`, `Project`, `SecretFile`.

**Deferred to Phase 6 (do NOT build here):** lease acquire/heartbeat/release, retry loop and the `SSH`/`HOST_LOST`/`TIMEOUT`/`COLLECTION`/`INTERNAL` failure classification (those wrap `execute_attempt`); timeout enforcement and process termination (§8.1); `ray.remote` decoration; assembling `JobResult` / `result.json`. `execute_attempt` runs the runner to completion (no timeout) and classifies only the command outcome (`COMMAND` / `OUTPUT_MISSING` / success); transport/setup failures propagate as exceptions for the Phase 6 wrapper to classify.

---

### Task 1: `RemoteLayout.run_dir` + `RunPaths`

**Files:**
- Modify: `src/ray_dispatcher/provisioning.py` (add `RunPaths` dataclass + two methods on `RemoteLayout`)
- Test: `tests/unit/test_run_paths.py`

**Interfaces:**
- Consumes: existing `RemoteLayout` (its `self.root`).
- Produces:
  - `RemoteLayout.run_dir(self, batch_id: str, job_id: str, attempt: int) -> str` → `f"{self.root}/runs/{batch_id}/{job_id}/{attempt}"` (spec §6.1).
  - `RunPaths` frozen dataclass with `base: str` and read-only properties: `run_root` (`{base}/run`), `venv` (`{run_root}/.venv`), `manifest` (`{base}/manifest.json`), `stdout` (`{base}/stdout.log`), `stderr` (`{base}/stderr.log`), `pid` (`{base}/pid.json`), `result` (`{base}/result.json`).
  - `RemoteLayout.run_paths(self, batch_id: str, job_id: str, attempt: int) -> RunPaths` → `RunPaths(self.run_dir(batch_id, job_id, attempt))`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_run_paths.py`:

```python
from ray_dispatcher.provisioning import RemoteLayout, RunPaths


def test_run_dir_under_root():
    lo = RemoteLayout("/home/ubuntu", "dfaas")
    assert lo.run_dir("b1", "jobA", 2) == "/home/ubuntu/.ray_dispatcher/runs/b1/jobA/2"


def test_run_paths_layout():
    rp = RunPaths("/home/ubuntu/.ray_dispatcher/runs/b1/jobA/2")
    assert rp.run_root == "/home/ubuntu/.ray_dispatcher/runs/b1/jobA/2/run"
    assert rp.venv == "/home/ubuntu/.ray_dispatcher/runs/b1/jobA/2/run/.venv"
    assert rp.manifest.endswith("/2/manifest.json")
    assert rp.stdout.endswith("/2/stdout.log")
    assert rp.stderr.endswith("/2/stderr.log")
    assert rp.pid.endswith("/2/pid.json")
    assert rp.result.endswith("/2/result.json")


def test_run_paths_from_layout():
    lo = RemoteLayout("/home/ubuntu", "dfaas")
    rp = lo.run_paths("b1", "jobA", 2)
    assert rp.base == lo.run_dir("b1", "jobA", 2)
    assert rp.run_root.endswith("/2/run")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_run_paths.py -v`
Expected: FAIL with `ImportError: cannot import name 'RunPaths'`.

- [ ] **Step 3: Write the implementation**

`provisioning.py` already imports `from dataclasses import dataclass, field`. Add the `RunPaths` dataclass at module level immediately BEFORE `class RemoteLayout:`:

```python
@dataclass(frozen=True)
class RunPaths:
    """Absolute remote paths inside one attempt's run dir (spec §6.1, §7).

    Control files (manifest/logs/pid/result) live in ``base``; the job runs in
    ``run_root`` (a copy of the provisioned source) so they never pollute outputs.
    """

    base: str

    @property
    def run_root(self) -> str:
        return f"{self.base}/run"

    @property
    def venv(self) -> str:
        return f"{self.run_root}/.venv"

    @property
    def manifest(self) -> str:
        return f"{self.base}/manifest.json"

    @property
    def stdout(self) -> str:
        return f"{self.base}/stdout.log"

    @property
    def stderr(self) -> str:
        return f"{self.base}/stderr.log"

    @property
    def pid(self) -> str:
        return f"{self.base}/pid.json"

    @property
    def result(self) -> str:
        return f"{self.base}/result.json"
```

Add these two methods to `RemoteLayout` (after the existing `uv_bin` method):

```python
    def run_dir(self, batch_id: str, job_id: str, attempt: int) -> str:
        return f"{self.root}/runs/{batch_id}/{job_id}/{attempt}"

    def run_paths(self, batch_id: str, job_id: str, attempt: int) -> RunPaths:
        return RunPaths(self.run_dir(batch_id, job_id, attempt))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_run_paths.py -v`
Expected: all three cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/provisioning.py tests/unit/test_run_paths.py
git commit -m "feat: add RemoteLayout.run_dir + RunPaths (remote attempt layout, §6.1/§7)"
```

---

### Task 2: `ssh.write_remote_file` (shared no-shell atomic remote write)

**Files:**
- Modify: `src/ray_dispatcher/ssh.py` (add module-level `write_remote_file`)
- Test: `tests/unit/test_ssh_write_remote_file.py`

**Interfaces:**
- Consumes: `Transport` (its `run`), `TransportError` (existing in `ssh.py`), `shlex` (stdlib).
- Produces: `write_remote_file(transport: Transport, path: str, content: str, *, mode: int | None = None) -> None` — writes `content` atomically to the absolute remote `path` via `printf %s <quoted> > <tmp>[ && chmod] && mv -f <tmp> <path>` in a single `sh -c`. Raises `TransportError` on a nonzero result. This is the §7 no-shell write primitive (data never reaches the shell except through `printf %s` with a `shlex`-quoted argument).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_ssh_write_remote_file.py`:

```python
import pytest

from ray_dispatcher.ssh import CommandResult, FakeTransport, TransportError, write_remote_file


def _runs(t):
    return [c[1] for c in t.calls if c[0] == "run"]


def test_write_remote_file_is_printf_tmp_then_mv():
    t = FakeTransport()  # default rc 0
    write_remote_file(t, "/home/u/.ray_dispatcher/runs/b/j/1/manifest.json", '{"a":1}')
    script = _runs(t)[-1][2]  # argv is ["sh", "-c", script]
    assert "printf %s" in script
    assert "manifest.json.tmp" in script
    assert "mv -f" in script
    # the data is shlex-quoted, not bare-interpolated into the shell
    assert "'{\"a\":1}'" in script


def test_write_remote_file_applies_mode():
    t = FakeTransport()
    write_remote_file(t, "/x/f", "data", mode=0o600)
    script = _runs(t)[-1][2]
    assert "chmod 600" in script


def test_write_remote_file_raises_on_failure():
    def results(argv):
        return CommandResult(1, "", "disk full", 0.0)

    t = FakeTransport(run_results=results)
    with pytest.raises(TransportError, match="disk full"):
        write_remote_file(t, "/x/f", "data")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_ssh_write_remote_file.py -v`
Expected: FAIL with `ImportError: cannot import name 'write_remote_file'`.

- [ ] **Step 3: Write the implementation**

Ensure `import shlex` is present in `ssh.py`'s stdlib import group (add it if absent). Append `write_remote_file` at module level (e.g. after `build_rsync_argv` or near `terminate_process_group`):

```python
def write_remote_file(
    transport: Transport, path: str, content: str, *, mode: int | None = None
) -> None:
    """Atomically write ``content`` to absolute remote ``path`` (spec §7 no-shell).

    Data reaches the shell only as a single shlex-quoted ``printf %s`` argument;
    the temp file is renamed into place so a partial write is never observed.
    """
    qtmp = shlex.quote(f"{path}.tmp")
    chmod = f" && chmod {mode:o} {qtmp}" if mode is not None else ""
    cmd = f"printf %s {shlex.quote(content)} > {qtmp}{chmod} && mv -f {qtmp} {shlex.quote(path)}"
    result = transport.run(["sh", "-c", cmd])
    if result.returncode != 0:
        raise TransportError(f"failed to write {path}: {result.stderr}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_ssh_write_remote_file.py -v`
Expected: all three cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/ssh.py tests/unit/test_ssh_write_remote_file.py
git commit -m "feat: add ssh.write_remote_file (shared no-shell atomic remote write, §7)"
```

---

### Task 3: `secret_env_map`

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (add `secret_env_map`; extend imports)
- Test: `tests/unit/test_secret_env_map.py`

**Interfaces:**
- Consumes: `models.Project`, `provisioning.RemoteLayout` (its `.secrets`).
- Produces: `secret_env_map(project: Project, layout: RemoteLayout) -> dict[str, str]` — maps each secret's `env_var` to its absolute remote path `f"{layout.secrets}/{remote_name}"`; secrets with `env_var is None` are skipped (copied but not exported, §4.2).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_secret_env_map.py`:

```python
from ray_dispatcher.models import Project, SecretFile
from ray_dispatcher.provisioning import RemoteLayout
from ray_dispatcher.scheduling import secret_env_map


def _project(secrets):
    return Project(path="/p", project_id="dfaas", python="3.10.18",
                   uv_version="0.11.25", secrets=secrets)


def test_maps_env_var_to_remote_secret_path():
    lo = RemoteLayout("/home/ubuntu", "dfaas")
    p = _project((SecretFile(source="~/g.lic", remote_name="g.lic", env_var="GRB_LICENSE_FILE"),))
    assert secret_env_map(p, lo) == {
        "GRB_LICENSE_FILE": "/home/ubuntu/.ray_dispatcher/secrets/dfaas/g.lic"
    }


def test_skips_secrets_without_env_var():
    lo = RemoteLayout("/home/ubuntu", "dfaas")
    p = _project((SecretFile(source="~/a", remote_name="a", env_var=None),))
    assert secret_env_map(p, lo) == {}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_secret_env_map.py -v`
Expected: FAIL with `ImportError: cannot import name 'secret_env_map'`.

- [ ] **Step 3: Write the implementation**

Add imports to `scheduling.py`. Merge into the existing first-party group (do NOT duplicate `from .errors import ...`):

```python
from .models import Project
from .provisioning import RemoteLayout
```

Append `secret_env_map` at module level (after `reconcile_host`, before `class LeaseService`):

```python
def secret_env_map(project: Project, layout: RemoteLayout) -> dict[str, str]:
    """Map each declared secret's env var to its absolute remote path (spec §4.2).

    Secrets without an ``env_var`` are provisioned but not exported into the job.
    """
    return {
        s.env_var: f"{layout.secrets}/{s.remote_name}"
        for s in project.secrets
        if s.env_var is not None
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_secret_env_map.py -v`
Expected: both cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/scheduling.py tests/unit/test_secret_env_map.py
git commit -m "feat: add secret_env_map (env var -> remote secret path, §4.2)"
```

---

### Task 4: `HostRuntime` + `build_runner_manifest`

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (add `HostRuntime`, `build_runner_manifest`; extend imports)
- Test: `tests/unit/test_runner_manifest.py`

**Interfaces:**
- Consumes: `models.Job`, `provisioning.RemoteLayout` + `RunPaths`, `posixpath` (stdlib), `Mapping` (already imported).
- Produces:
  - `HostRuntime` frozen dataclass: `host: str`, `layout: RemoteLayout`, `environment_digest: str`, `runner_digest: str`, `project_path: str`, `secret_env: Mapping[str, str]`. The immutable per-host facts the backend assembles once after provisioning and reuses for every attempt on that host.
  - `build_runner_manifest(job: Job, *, run_root: str, venv: str, run: RunPaths, secret_env: Mapping[str, str]) -> dict[str, object]` — the dict consumed by `remote_runner.py`. `cwd` is `posixpath.normpath(f"{run_root}/{job.cwd}")` (so `"."` → `run_root`); `venv_bin` is `f"{venv}/bin"`; `virtual_env` is `venv`; log/pid/result paths come from `run`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_runner_manifest.py`:

```python
from ray_dispatcher.models import Job
from ray_dispatcher.provisioning import RunPaths
from ray_dispatcher.scheduling import build_runner_manifest


def test_manifest_keys_match_remote_runner():
    run = RunPaths("/home/u/.ray_dispatcher/runs/b/jobA/1")
    job = Job(id="jobA", command=("python", "run.py", "--n", "5"),
              env={"FOO": "bar"}, cwd="sub/dir")
    m = build_runner_manifest(
        job, run_root=run.run_root, venv="/env/.venv", run=run,
        secret_env={"GRB_LICENSE_FILE": "/secrets/g.lic"},
    )
    assert m["argv"] == ["python", "run.py", "--n", "5"]
    assert m["cwd"] == "/home/u/.ray_dispatcher/runs/b/jobA/1/run/sub/dir"
    assert m["env"] == {"FOO": "bar"}
    assert m["secret_env"] == {"GRB_LICENSE_FILE": "/secrets/g.lic"}
    assert m["venv_bin"] == "/env/.venv/bin"
    assert m["virtual_env"] == "/env/.venv"
    assert m["stdout_path"] == run.stdout
    assert m["stderr_path"] == run.stderr
    assert m["pid_path"] == run.pid
    assert m["result_path"] == run.result
    # exactly the keys remote_runner.py reads — no more, no less
    assert set(m) == {"argv", "cwd", "env", "secret_env", "venv_bin",
                      "virtual_env", "stdout_path", "stderr_path", "pid_path", "result_path"}


def test_manifest_cwd_dot_is_run_root():
    run = RunPaths("/r/1")
    job = Job(id="j", command=("echo", "hi"))  # cwd defaults to "."
    m = build_runner_manifest(job, run_root=run.run_root, venv="/v", run=run, secret_env={})
    assert m["cwd"] == "/r/1/run"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_runner_manifest.py -v`
Expected: FAIL with `ImportError: cannot import name 'build_runner_manifest'`.

- [ ] **Step 3: Write the implementation**

Add `import posixpath` to `scheduling.py`'s stdlib import group, and extend the `provisioning` import to include `RunPaths`:

```python
from .provisioning import RemoteLayout, RunPaths
```

(merge with the line added in Task 3 — one `from .provisioning import ...` line). Add to the `models` import: `Job` (so the line reads `from .models import Job, Project`). Append:

```python
@dataclass(frozen=True)
class HostRuntime:
    """Immutable per-host execution context, assembled once after provisioning."""

    host: str
    layout: RemoteLayout
    environment_digest: str
    runner_digest: str
    project_path: str
    secret_env: Mapping[str, str]


def build_runner_manifest(
    job: Job,
    *,
    run_root: str,
    venv: str,
    run: RunPaths,
    secret_env: Mapping[str, str],
) -> dict[str, object]:
    """Build the JSON manifest consumed by remote_runner.py (spec §7.5).

    The job argv and env travel as data here; remote_runner Popens argv with no
    shell, prepends venv_bin to PATH, sets VIRTUAL_ENV, and exports secret_env.
    """
    return {
        "argv": list(job.command),
        "cwd": posixpath.normpath(f"{run_root}/{job.cwd}"),
        "env": dict(job.env),
        "secret_env": dict(secret_env),
        "venv_bin": f"{venv}/bin",
        "virtual_env": venv,
        "stdout_path": run.stdout,
        "stderr_path": run.stderr,
        "pid_path": run.pid,
        "result_path": run.result,
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_runner_manifest.py -v`
Expected: both cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/scheduling.py tests/unit/test_runner_manifest.py
git commit -m "feat: add HostRuntime + build_runner_manifest (§7.5)"
```

---

### Task 5: `execute_attempt` (the §7.2–7.9 driver)

**Files:**
- Modify: `src/ray_dispatcher/scheduling.py` (add `_run_checked` + `execute_attempt`; extend imports)
- Test: `tests/unit/test_execute_attempt.py`

**Interfaces:**
- Consumes: `HostRuntime`, `build_runner_manifest`, `RunPaths` (Task 1/4); `ssh.write_remote_file`, `ssh.Transport`; `results.JobLayout`, `results.create_attempt_dir`, `results.collect_outputs`, `results.publish_job_outputs`, `results.write_attempt_json` (Phase 5a); `models.Job`, `AttemptResult`, `JobStatus`, `FailureKind`; `errors.DispatcherError`; stdlib `os`, `posixpath`, `shlex`, `json`.
- Produces:
  - `_run_checked(transport: Transport, argv: list[str], what: str) -> CommandResult` — runs `argv`, raises `DispatcherError` on nonzero (used for the setup commands and the result read).
  - `execute_attempt(transport: Transport, runtime: HostRuntime, job: Job, *, batch_id: str, attempt: int, local: JobLayout) -> AttemptResult` — runs one attempt end-to-end (§7.2–7.9) and returns the `AttemptResult`. Side effects: creates the local attempt dir + logs + `attempt.json`; on success, publishes outputs to `local.outputs_dir`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_execute_attempt.py`:

```python
import json

from ray_dispatcher.models import AttemptResult, FailureKind, InputSpec, Job, JobStatus, OutputSpec
from ray_dispatcher.provisioning import RemoteLayout
from ray_dispatcher.results import JobLayout
from ray_dispatcher.scheduling import HostRuntime, execute_attempt
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _runtime():
    return HostRuntime(
        host="10.0.0.1",
        layout=RemoteLayout("/home/u", "dfaas"),
        environment_digest="env123",
        runner_digest="run123",
        project_path="/local/proj",
        secret_env={"GRB_LICENSE_FILE": "/home/u/.ray_dispatcher/secrets/dfaas/g.lic"},
    )


def _transport(returncode=0):
    # Programmable fake: the runner's result.json reports `returncode`; every
    # other remote command (mkdir/rsync/ln/runner) succeeds.
    def results(argv):
        if argv[0] == "cat" and argv[1].endswith("result.json"):
            doc = json.dumps({"returncode": returncode, "started_at": 1.0,
                              "ended_at": 2.0, "duration_s": 1.5})
            return CommandResult(0, doc, "", 0.0)
        return CommandResult(0, "", "", 0.0)
    return FakeTransport(run_results=results)


def _layout(tmp_path):
    return JobLayout(str(tmp_path / "results"), "b1", "jobA")


def _runs(t):
    return [c[1] for c in t.calls if c[0] == "run"]


def test_success_with_no_outputs_publishes_and_succeeds(tmp_path):
    job = Job(id="jobA", command=("python", "run.py"))  # no declared outputs
    t = _transport(returncode=0)
    res = execute_attempt(t, _runtime(), job, batch_id="b1", attempt=1, local=_layout(tmp_path))
    assert isinstance(res, AttemptResult)
    assert res.status is JobStatus.SUCCEEDED
    assert res.returncode == 0
    assert res.failure_kind is None
    assert res.host == "10.0.0.1"
    assert res.duration_s == 1.5
    # logs recorded locally; outputs published (empty) to the job outputs dir
    lo = _layout(tmp_path)
    assert res.stdout_log == str(lo.stdout_log(1))
    assert lo.outputs_dir.is_dir()                 # published
    assert lo.attempt_json(1).is_file()            # attempt.json written


def test_command_failure_classifies_command_and_does_not_publish(tmp_path):
    job = Job(id="jobA", command=("python", "run.py"))
    t = _transport(returncode=3)
    res = execute_attempt(t, _runtime(), job, batch_id="b1", attempt=1, local=_layout(tmp_path))
    assert res.status is JobStatus.FAILED
    assert res.returncode == 3
    assert res.failure_kind is FailureKind.COMMAND
    assert not _layout(tmp_path).outputs_dir.exists()  # no publish on failure


def test_missing_required_output_classifies_output_missing(tmp_path):
    # FakeTransport.pull is a no-op, so a required output never lands -> OUTPUT_MISSING
    job = Job(id="jobA", command=("python", "run.py"),
              outputs=(OutputSpec(source="solutions/out.json", required=True),))
    t = _transport(returncode=0)
    res = execute_attempt(t, _runtime(), job, batch_id="b1", attempt=1, local=_layout(tmp_path))
    assert res.status is JobStatus.FAILED
    assert res.failure_kind is FailureKind.OUTPUT_MISSING
    assert not _layout(tmp_path).outputs_dir.exists()


def test_remote_protocol_is_no_shell_and_ordered(tmp_path):
    job = Job(id="jobA", command=("python", "run.py", "--cfg", "c.json"),
              inputs=(InputSpec(source="c.json", destination="c.json"),))
    t = _transport(returncode=0)
    execute_attempt(t, _runtime(), job, batch_id="b1", attempt=1, local=_layout(tmp_path))
    runs = _runs(t)
    flat = [tok for argv in runs for tok in argv]
    # the runner is invoked by absolute python3 + runner path + manifest path
    assert any(a[0] == "python3" and a[1].endswith("/bin/run123/remote_runner.py")
               and a[2].endswith("/1/manifest.json") for a in runs)
    # §7 no-shell: job command tokens never appear as their own argv anywhere;
    # they travel only inside the manifest JSON (written via printf %s).
    assert "run.py" not in flat
    assert "--cfg" not in flat
    # the run dir leaf is created without -p (existing dir is an error, §7.2)
    assert any("mkdir " in a[2] and "-p" not in a[2].split("&&")[-1] for a in runs
               if a[0] == "sh")
    # the input was pushed (host->VM), not shelled
    pushes = [c[1] for c in t.calls if c[0] == "push"]
    assert any(p[0] == "/local/proj/c.json" and p[1].endswith("/run/c.json") for p in pushes)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_execute_attempt.py -v`
Expected: FAIL with `ImportError: cannot import name 'execute_attempt'`.

- [ ] **Step 3: Write the implementation**

Extend `scheduling.py` imports. Add stdlib `import os` (with `posixpath` from Task 4 and `shlex`) and `import shlex` if absent; extend the `models` import to `from .models import AttemptResult, FailureKind, Job, JobStatus, Project`; add `from .errors import DispatcherError, ModelValidationError, NoHealthyHostsError` (merge `DispatcherError` into the existing errors line); extend the `results` import with what is needed; and extend the `ssh` import:

```python
from .errors import DispatcherError, ModelValidationError, NoHealthyHostsError
from .results import (
    JobLayout,
    collect_outputs,
    create_attempt_dir,
    publish_job_outputs,
    write_attempt_json,
)
from .ssh import CommandResult, Transport, terminate_process_group, write_remote_file
```

(`CommandResult` is the return type of `_run_checked`; `JobLayout`/`collect_outputs`/etc. come from `results.py`.) Append `_run_checked` and `execute_attempt` at module level after `build_runner_manifest`:

```python
def _run_checked(transport: Transport, argv: list[str], what: str) -> CommandResult:
    """Run a library-controlled remote command; raise on nonzero (no user strings)."""
    result = transport.run(argv)
    if result.returncode != 0:
        raise DispatcherError(f"{what} failed: {result.stderr.strip()}")
    return result


def execute_attempt(
    transport: Transport,
    runtime: HostRuntime,
    job: Job,
    *,
    batch_id: str,
    attempt: int,
    local: JobLayout,
) -> AttemptResult:
    """Run one job attempt on one provisioned host over SSH (spec §7 steps 2-9).

    Returns the AttemptResult and, on success, publishes outputs to the job's
    local outputs dir. Setup/transport failures propagate for the Phase 6 wrapper
    to classify (SSH/HOST_LOST/INTERNAL); this driver classifies only the command
    outcome (COMMAND / OUTPUT_MISSING / success).
    """
    run = runtime.layout.run_paths(batch_id, job.id, attempt)
    venv = runtime.layout.env_venv(runtime.environment_digest)
    runner = runtime.layout.runner(runtime.runner_digest)
    attempt_dir = create_attempt_dir(local, attempt)  # local (§9.1)

    # §7.2 fresh remote run dir; the leaf mkdir (no -p) errors if it exists.
    parent = posixpath.dirname(run.base)
    _run_checked(
        transport,
        ["sh", "-c", f"mkdir -p {shlex.quote(parent)} && mkdir {shlex.quote(run.base)} "
                     f"&& mkdir {shlex.quote(run.run_root)}"],
        "create run dir",
    )
    # §7.3 copy provisioned source on the VM; .venv -> the immutable env.
    _run_checked(
        transport,
        ["sh", "-c", f"rsync -a {shlex.quote(runtime.layout.source)}/ {shlex.quote(run.run_root)}/ "
                     f"&& ln -s {shlex.quote(venv)} {shlex.quote(run.venv)}"],
        "copy source",
    )
    # §7.4 push each input to its explicit (normalized, run-root-relative) destination.
    for inp in job.inputs:
        remote_dest = f"{run.run_root}/{inp.destination}"
        _run_checked(
            transport,
            ["sh", "-c", f"mkdir -p {shlex.quote(posixpath.dirname(remote_dest))}"],
            "input dir",
        )
        local_src = inp.source if os.path.isabs(inp.source) else os.path.join(
            runtime.project_path, inp.source
        )
        transport.push(local_src, remote_dest)
    # §7.5 write the runner manifest (no shell assembled from user strings).
    manifest = build_runner_manifest(
        job, run_root=run.run_root, venv=venv, run=run, secret_env=runtime.secret_env
    )
    write_remote_file(transport, run.manifest, json.dumps(manifest))
    # §7.6 invoke the versioned runner (it Popens argv, records pid/pgid). No
    # timeout here: enforcement + termination are Phase 6 (§8.1).
    _run_checked(transport, ["python3", runner, run.manifest], "invoke runner")
    # parse the runner's result.json (returncode + monotonic duration, §7).
    info = json.loads(_run_checked(transport, ["cat", run.result], "read result").stdout)
    returncode = int(info["returncode"])
    duration_s = float(info["duration_s"])
    # §7.7 pull streamed logs into the local attempt dir.
    transport.pull(run.stdout, str(local.stdout_log(attempt)))
    transport.pull(run.stderr, str(local.stderr_log(attempt)))
    # §7.8 collect declared outputs into attempt-scoped staging.
    staging = attempt_dir / "outputs"
    staging.mkdir()
    collected = collect_outputs(transport, run.run_root, job.outputs, staging)
    # classify: a command failure dominates; otherwise a missing required output
    # is OUTPUT_MISSING (§7.8 / §9.3).
    if returncode != 0:
        status, failure_kind = JobStatus.FAILED, FailureKind.COMMAND
    elif collected.missing_required:
        status, failure_kind = JobStatus.FAILED, FailureKind.OUTPUT_MISSING
    else:
        status, failure_kind = JobStatus.SUCCEEDED, None
    if status is JobStatus.SUCCEEDED:
        publish_job_outputs(staging, local.outputs_dir)  # §7.9 atomic publish
    result = AttemptResult(
        number=attempt,
        host=runtime.host,
        status=status,
        returncode=returncode,
        duration_s=duration_s,
        stdout_log=str(local.stdout_log(attempt)),
        stderr_log=str(local.stderr_log(attempt)),
        failure_kind=failure_kind,
        error=None,
    )
    write_attempt_json(
        local.attempt_json(attempt), result, missing_optional=collected.missing_optional
    )
    return result
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_execute_attempt.py -v`
Expected: all four cases `passed`.

- [ ] **Step 5: Commit**

```bash
git add src/ray_dispatcher/scheduling.py tests/unit/test_execute_attempt.py
git commit -m "feat: add execute_attempt host-side single-attempt driver (§7.2-7.9)"
```

---

### Task 6: Phase 5b gate

**Files:**
- (no new source) — final full-toolchain verification.

**Interfaces:** none.

- [ ] **Step 1: Run the full suite**

Run: `uv run pytest -q`
Expected: all tests `passed` (Phases 1–5a + Phase 5b).

- [ ] **Step 2: Lint + type check**

Run: `uv run ruff check --fix . && uv run ruff check . && uv run mypy`
Expected: ruff auto-sorts then `All checks passed!`; mypy `Success: no issues found`.

If mypy flags the `dict[str, object]` manifest or the `Mapping` field, fix minimally (no config relaxation). Confirm `scheduling.py` still does NOT import `ray` (the actor/task decoration is Phase 6).

- [ ] **Step 3: Commit (only if Step 2 made changes)**

```bash
git add -A
git commit -m "chore: phase 5b gate green (ruff + mypy)"
```

---

## Phase 5b self-review

Run before declaring Phase 5b done:

- [ ] `RemoteLayout.run_dir`/`RunPaths` give the §6.1 run-dir layout; control files in `base`, the job tree in `run_root`.
- [ ] `ssh.write_remote_file` writes via `printf %s` of a `shlex`-quoted argument, temp-then-`mv` atomic, raises `TransportError` on failure.
- [ ] `secret_env_map` maps `env_var` → remote secret path and skips `env_var is None` (§4.2).
- [ ] `build_runner_manifest` emits exactly the 10 keys `remote_runner.py` reads; `cwd` joins `run_root` with `job.cwd` (`"."` → `run_root`); `venv_bin`/`virtual_env` set.
- [ ] `execute_attempt` follows §7 order: fresh run dir (leaf `mkdir` without `-p`, existing is an error), source copy + `.venv` symlink, input push, manifest write, runner invoke, result parse, log pull, output collect, classify, publish-on-success only; writes `attempt.json` with `missing_optional`.
- [ ] **§7 no-shell:** job `command`/`env`/paths never appear as shell tokens — only inside the manifest JSON (verified by the protocol test asserting `run.py`/`--cfg` are absent from every run argv). Every interpolated path is `shlex`-quoted.
- [ ] `COMMAND` (rc≠0) dominates; `OUTPUT_MISSING` only when the command succeeded but a required output is absent; success publishes outputs atomically and records none.
- [ ] `uv run pytest -q`, `uv run ruff check .`, `uv run mypy` all green; `scheduling.py` does not import `ray`.

**Deliverable:** `execute_attempt` (+ `HostRuntime`, `build_runner_manifest`, `secret_env_map`, `_run_checked`) in `scheduling.py`; `RemoteLayout.run_dir`/`RunPaths` in `provisioning.py`; `ssh.write_remote_file`. The complete host-side mechanics to run one attempt and record its result — ready for the Phase 6 Ray task to wrap with lease + retry.

**Residuals carried to Phase 6 (documented):**
- **Lease + Ray task:** wrap `execute_attempt` with `HostLease.acquire`/`heartbeat`/`release`, the §8.3 retry loop, and `ray.remote(num_cpus=0)`; map exceptions from `execute_attempt` (transport/setup failures) to `SSH`/`HOST_LOST`/`INTERNAL` and collection failures to `COLLECTION`.
- **Timeout + termination (§8.1):** enforce `job.timeout_s` (control-SSH `SIGTERM`→grace→`SIGKILL` of the recorded pgid, via `terminate_process_group`/`reconcile_host`), classify `TIMEOUT`.
- **`JobResult` assembly:** the backend builds `JobResult` (final attempt's `returncode`/`host`, `output_dir = str(local.outputs_dir)` on success, the full `attempts` tuple) and writes the job `result.json` via `write_result_json`.
- **`ssh.write_remote_file` adoption:** `provisioning.HostProvisioner._write_remote_file` predates this shared primitive; it can adopt `write_remote_file` in a later cleanup (both are independently tested; not done here to avoid destabilizing provisioning).
