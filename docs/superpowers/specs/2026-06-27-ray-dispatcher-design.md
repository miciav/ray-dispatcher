# ray-dispatcher — Design

- **Date:** 2026-06-27
- **Status:** Approved (ready for implementation planning)
- **Author:** Michele Ciavotta (with Claude Code)

## 1. Overview

`ray_dispatcher` is a Python library, usable by other tools, that dispatches
batches of experiments onto a small pool of SSH-accessible virtual machines,
runs them, monitors progress, and downloads the results back to the host.

The library is a **generic job dispatcher**. A *job* is `{command, input files,
expected outputs}`. The caller submits one job or N jobs (a batch); the library
schedules them across the VMs, executes them, and collects their outputs. The
library has no knowledge of any specific experiment format (e.g. it does not
parse the target project's config files) — the granularity of a job is entirely
the caller's choice.

It uses **Ray** as the distributed scheduling engine, **uv** to reproduce the
execution environment on each VM, and **SSH + rsync** as the transport. It does
not reinvent these pieces.

### Driving use case

The first consumer is the `DFaasOptimizer` project (`../DFaasOptimizer`). A
single experiment there is a command such as:

```
python run.py --config config_files/eval_smoke.json --methods faas-madea --n_experiments 5
```

which reads a JSON config, runs one or more solver methods (Gurobi / pyomo), and
writes results into `solutions/<name>/`. The environment is heavy and strict:
Python `>=3.10,<3.11`, a fully pinned dependency set with `uv.lock`, and Gurobi
(a licensed commercial solver). The library must reproduce this environment
faithfully on each VM.

## 2. Goals and non-goals

### Goals
- Provide a clean Python API consumable by other tools.
- Full lifecycle on **bare VMs** (SSH-only): provision the environment, run the
  Ray scheduler, dispatch jobs, monitor, download results, tear down.
- Be **agnostic to job granularity**: the caller decides what a job is.
- Reproduce a heavy, pinned environment per VM via `uv` (Python pin + `uv.lock`).
- Reuse existing libraries (Ray, uv, Fabric, rsync) rather than re-implementing
  scheduling, environment resolution, or transport.
- Be testable end-to-end locally using the `multipass-sdk` library as a VM
  factory, with no runtime dependency on multipass.

### Non-goals (v1)
- No autoscaling or dynamic VM provisioning (the VM pool is fixed and given).
- No in-memory data passing between jobs (each job is an isolated subprocess).
- No CLI (Python API only for v1; a thin CLI is possible future work).
- No support for non-Linux VMs (VMs are assumed Debian/Ubuntu-like).
- The library does not obtain or validate the Gurobi license; the user provides
  a license file as a secret.

## 3. Architecture decision

### Chosen approach: Ray as a host-side scheduler, SSH/rsync as transport

Ray runs **on the host** (the machine driving the dispatch), not on the VMs.
The VMs are not part of a Ray cluster and do not have Ray installed. Each VM is
modelled in Ray as a **custom resource**; a Ray task represents one job and does
the SSH/rsync/exec work against a single VM.

Rationale:
- The workload is **subprocess experiments** (`python run.py ...`) that read
  files and write an output directory. Ray's distinguishing cluster features
  (object store, actors, in-memory data passing, GPU bin-packing, autoscaling)
  are largely unused by this workload. What is actually needed is a *distributed
  work queue*: N jobs → M VMs with K slots each, with scheduling, status,
  retries, and result collection.
- The **only guaranteed connectivity is SSH** (passwordless) from the host to
  each VM. A real Ray cluster on the VMs would require the VMs to reach each
  other on several direct TCP ports — a common failure point that the SSH-only
  approach avoids entirely.
- Adding Ray into the target project's tightly pinned `uv.lock` environment
  risks dependency conflicts. Keeping Ray on the host sidesteps this.

Ray still earns its place: it provides admission control (custom per-VM
resources), dynamic load balancing, futures/`as_completed`, task retries, and a
live dashboard.

### Seam for a future native-cluster backend

The dispatch layer sits behind an `ExecutionBackend` abstraction so a future
**native Ray cluster on the VMs** backend (Ray running on the workers, code
executed as Ray tasks on the VMs) can be added **without changing the public
API**. The v1 backend is `SshRayBackend`.

```python
class ExecutionBackend(ABC):
    def setup(self, inventory: Inventory, project: Project) -> None: ...   # topology + provisioning
    def submit(self, job: Job) -> "ray.ObjectRef": ...                     # future -> JobResult
    def teardown(self) -> None: ...
```

Both backends return **Ray `ObjectRef` futures**, so `Dispatcher` performs
`ray.wait` / `as_completed` identically regardless of backend.

## 4. Core concepts and public API

Three concepts with distinct lifecycles:

- **`RemoteHost` / `Inventory`** — the pool of SSH-passwordless VMs. Each host
  declares `slots` = the number of concurrent jobs it tolerates.
- **`Project`** — the heavy environment reproduced **once per VM**: a local
  directory containing `pyproject.toml` + `uv.lock`. Provisioning = rsync the
  project + `uv sync`. Stable; shared by all jobs.
- **`Job`** — the light, generic unit: a command + input/config files that
  change + output paths to download. Submit one or N; all reuse the provisioned
  `Project`.

### Usage sketch

```python
from ray_dispatcher import Inventory, RemoteHost, Project, Job, Dispatcher

inventory = Inventory([
    RemoteHost("10.0.0.11", user="ubuntu", slots=2),   # max 2 concurrent jobs
    RemoteHost("10.0.0.12", user="ubuntu", slots=2),
])  # or Inventory.from_yaml("hosts.yaml")

project = Project(
    path="../DFaasOptimizer",                   # contains pyproject.toml + uv.lock
    python="3.10",
    secrets={"gurobi.lic": "~/lic/gurobi.lic"}, # -> GRB_LICENSE_FILE on the VM
    exclude=["solutions/", ".venv/", ".git/"],  # rsync excludes
)

jobs = [
    Job(id="madea-smoke",
        command=["python", "run.py", "--config", "config_files/eval_smoke.json",
                 "--methods", "faas-madea", "--n_experiments", "5"],
        inputs=["config_files/eval_smoke.json"],   # files (re)shipped for this job
        outputs=["solutions/eval_smoke/"])         # paths to download back
    # ... more
]

with Dispatcher(inventory, project, results_dir="./results",
                max_retries=1, raise_on_failure=False) as d:
    d.provision()              # uv + Python 3.10 + uv sync + secrets — once per VM
    results = d.run(jobs)      # schedule across VMs, monitor, download outputs
    # streaming alternative: for r in d.as_completed(d.submit(jobs)): ...

for r in results:
    print(r.id, r.status, r.returncode, r.duration_s, r.output_dir)
```

### Data model

```python
@dataclass
class RemoteHost:
    host: str
    user: str
    slots: int = 1
    port: int = 22
    identity_file: str | None = None   # default: SSH agent / ~/.ssh config

@dataclass
class Inventory:
    hosts: list[RemoteHost]
    @classmethod
    def from_yaml(cls, path: str) -> "Inventory": ...

@dataclass
class Project:
    path: str                          # local dir with pyproject.toml + uv.lock
    python: str = "3.10"
    secrets: dict[str, str] = field(default_factory=dict)  # dest_name -> local_path
    exclude: list[str] = field(default_factory=lambda: [".venv/", ".git/"])
    name: str | None = None            # default: derived from path; used in remote paths

@dataclass
class Job:
    id: str
    command: list[str]
    inputs: list[str] = field(default_factory=list)    # local paths (relative to Project.path,
                                                       # or absolute); placed at the same relative
                                                       # path inside the run dir on the VM
    outputs: list[str] = field(default_factory=list)   # paths relative to the run dir on the VM,
                                                       # downloaded to ./results/<job_id>/
    env: dict[str, str] = field(default_factory=dict)
    timeout_s: float | None = None
    cwd: str | None = None             # working dir for the command; default: run dir root

class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

@dataclass
class JobResult:
    id: str
    status: JobStatus
    returncode: int | None
    duration_s: float
    host: str | None
    output_dir: str | None             # local dir where outputs were downloaded
    stdout_log: str | None
    stderr_log: str | None
    attempts: int
    error: str | None = None
```

### `Dispatcher`

```python
class Dispatcher:
    def __init__(self, inventory: Inventory, project: Project, *,
                 results_dir: str = "./results",
                 max_retries: int = 1,
                 raise_on_failure: bool = False,
                 backend: ExecutionBackend | None = None): ...  # default: SshRayBackend

    def provision(self, *, force: bool = False) -> None: ...     # idempotent, once per VM
    def submit(self, jobs: list[Job]) -> list["ray.ObjectRef"]: ...
    def as_completed(self, refs: list["ray.ObjectRef"]) -> Iterator[JobResult]: ...
    def run(self, jobs: list[Job]) -> list[JobResult]: ...       # blocking; submit + drain
    def teardown(self, *, purge: bool = False) -> None: ...      # ray.shutdown; optional remote cleanup
    def __enter__(self) -> "Dispatcher": ...
    def __exit__(self, *exc) -> None: ...                        # calls teardown()
```

`run()` auto-calls `provision()` if it has not been run.

## 5. Module breakdown

Each module has a single responsibility:

```
ray_dispatcher/
├── models.py        # dataclasses: RemoteHost, Inventory, Project, Job, JobResult, JobStatus
├── ssh.py           # the ONLY module that touches SSH: run-command, rsync-push, rsync-pull, stream-log
├── provisioning.py  # prepare a VM: install uv -> Python 3.10 -> rsync project -> uv sync -> secrets
├── scheduling.py    # host -> Ray custom resources, Ray job task, HostLease actor, per-VM slots
├── results.py       # output layout + collection on the host (./results/<job_id>/...)
├── backends/
│   ├── base.py      # ExecutionBackend (ABC) — the seam
│   └── ssh_ray.py   # v1 backend: host-side Ray scheduler + SSH execution
├── dispatcher.py    # public orchestration: provision -> submit -> monitor -> collect -> retry -> teardown
└── __init__.py      # public exports
```

### Reused libraries (no reinvention)
- **Ray** (latest, pinned) — scheduling, futures, retries, dashboard.
- **Fabric** (over paramiko) — SSH command execution in `ssh.py`.
- **rsync** (via subprocess) — incremental bulk transport of directories
  (e.g. `solutions/`); more efficient than SFTP for directory trees.
- **uv** — invoked over SSH by `provisioning.py` to recreate the environment.
- **`multipass-sdk`** (test only) — VM factory for e2e tests; no runtime dependency.

## 6. Execution flow

### A. Provisioning a VM (idempotent, once)
1. Check `uv` presence; if missing, run the official installer over SSH.
2. `uv python install 3.10`.
3. rsync the `Project` to `~/.ray_dispatcher/projects/<hash>/`, applying excludes
   (`.venv/`, `solutions/`, `.git/`, ...). The `<hash>` (content/identity of the
   project) lets multiple projects coexist and makes re-provisioning incremental.
4. `uv sync --frozen` in that directory — recreates `.venv` **exactly** from
   `uv.lock` (fails if the lock is out of date, guaranteeing reproducibility).
5. Push `secrets` (e.g. `gurobi.lic`) to a known path; inject `GRB_LICENSE_FILE`
   into the job environment.
6. Write a sentinel file recording the project hash so subsequent runs skip work
   already done. `provision(force=True)` re-runs everything.

### B. Executing a single job (inside the Ray task, over SSH to one VM)
1. The Ray task only starts once a VM slot is free (admission control) and a
   concrete free host has been leased (see scheduling).
2. Create an isolated run directory on the VM:
   `~/.ray_dispatcher/runs/<job_id>/` = a **full copy of the project code**
   (not a hardlink), with the heavy immutable `.venv` **shared via symlink**.
   This isolates code and outputs between concurrent jobs on the same VM without
   duplicating gigabytes of dependencies.
3. rsync the job's `inputs` into the run directory (overwriting configs/inputs
   that change per job).
4. Execute `cd <run-dir> && <env> && <command>`, with the experiment running
   against the shared `.venv` (via `VIRTUAL_ENV`/`PATH` pointing at the symlinked
   venv). stdout/stderr are **streamed live** to `./results/<job_id>/stdout.log`
   and `stderr.log` on the host (tail-able). Capture exit code and duration.
   Enforce `timeout_s` if set.
5. rsync-pull the job's `outputs` from the run directory to
   `./results/<job_id>/`.
6. Run-dir cleanup is configurable; default keeps it on failure for debugging.
7. Return a `JobResult`.

### C. Scheduling and concurrency (Ray on the host), two levels
- **Admission**: a custom resource `vm_slot` with total capacity = Σ`slots`.
  Ray never starts more than Σslots jobs concurrently.
- **Host binding**: a **Ray actor `HostLease`** assigns each task a concretely
  free VM (respecting per-host `slots`) and releases it when the job finishes.
  This balances load dynamically — a job starts as soon as *any* VM has a free
  slot — rather than statically pinning jobs to hosts at submit time.

### D. Monitoring
- `run(jobs)` blocks and returns the full list of `JobResult`; alternatively
  `submit` + `as_completed` streams results as they finish.
- Per-job status: `PENDING -> RUNNING -> SUCCEEDED/FAILED`.
- Live per-job logs on the host; an optional progress view (X/N done, which
  hosts) backed by `rich` (optional dependency). The Ray dashboard URL is
  printed.

### E. Errors and retries
- A failure is a non-zero exit code, an SSH error, or a `timeout_s` breach.
- A failed job is retried up to `max_retries` (default **1**) on a **different
  VM** when possible, to absorb transient node problems.
- **Partial results are non-fatal**: a failed job does not abort the batch.
  `run()` returns all `JobResult`s; failed ones are marked with their captured
  logs. Default `raise_on_failure=False`.
- Teardown: `ray.shutdown()`. Provisioned environments **persist** between
  batches (subsequent runs are fast) unless `teardown(purge=True)`.

## 7. Testing strategy

Two levels:

- **Unit tests (no VMs).** `ssh.py` sits behind an interface so a fake SSH layer
  can be injected (mirroring `multipass-sdk`'s own `FakeBackend` pattern). These
  cover: provisioning command generation, `HostLease` logic, retry behaviour,
  results layout, and model parsing. Fast; run in CI.
- **End-to-end tests with `multipass-sdk`** (opt-in, `@pytest.mark.e2e`). A
  pytest fixture: `launch_many` of N multipass VMs → inject the host's SSH
  **public key via cloud-init** (`find_ssh_public_key` + `CloudInitConfig`) →
  `wait_ready` → build an `Inventory` from the VM IPs. Then a full round-trip:
  provision (uv install, Python 3.10, `uv sync`) + dispatch a few jobs + assert
  outputs were downloaded. Teardown = `delete(purge=True)`. The library never
  imports multipass; it is only the test's VM factory, and the exercised SSH
  path is identical to that of real remote VMs.
- **Synthetic fixture project.** A minimal `pyproject` whose trivial script reads
  a config and writes an output file — exercises the whole flow in seconds.
  `DFaasOptimizer` is too heavy (and Gurobi-licensed) for CI; it remains a manual
  smoke target.
- **Scheduling** is verified with Ray running locally + custom resources,
  asserting the per-host concurrency cap.

## 8. Packaging

- Layout: `src/ray_dispatcher/`, `tests/`, `examples/`.
- `pyproject.toml` with `requires-python = ">=3.10"`, **Ray pinned**, **Fabric**
  pinned, `rich` as an optional extra (`ray-dispatcher[progress]`),
  `multipass-sdk` as a dev/test dependency (`git+`).
- Managed with `uv`.
- Primary interface is the Python API. A thin CLI
  (`ray-dispatcher run --inventory hosts.yaml --jobs jobs.yaml`) is **future
  work**, not part of v1.

## 9. Assumptions and constraints

- Passwordless (key-based) SSH from the host to every VM is already configured
  (agent / `~/.ssh/config` / known_hosts).
- VMs are **Linux, Debian/Ubuntu-like** (uv installer, `cp -a`, POSIX symlinks).
  `rsync` is available on the host and on every VM.
- VMs have **outbound internet** at first provisioning (uv + package downloads).
- **Gurobi**: the user supplies the license as a secret file; the library
  installs `gurobipy` via uv and sets `GRB_LICENSE_FILE`. Obtaining and
  validating the license is the user's responsibility.
- Ray runs **on the host** as the scheduler (macOS is supported).
- Sufficient disk on each VM for per-job code copies plus outputs.

## 10. Future work

- `NativeRayClusterBackend` (Ray cluster on the VMs; experiments run as Ray
  tasks on the workers) behind the existing `ExecutionBackend` seam.
- A thin CLI for non-Python consumers.
- Offline / wheel-cache provisioning for VMs without outbound internet.
- Optional shared filesystem (NFS) support for very large inputs/outputs.
