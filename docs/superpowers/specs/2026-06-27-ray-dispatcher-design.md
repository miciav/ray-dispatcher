# ray-dispatcher — Design

- **Created:** 2026-06-27
- **Revised:** 2026-06-28
- **Status:** In review
- **Author:** Michele Ciavotta (with Claude Code)

## 1. Overview

`ray_dispatcher` is a Python library that dispatches batches of isolated
subprocess jobs onto a fixed pool of SSH-accessible virtual machines, monitors
them, and downloads their outputs to the host.

A job consists of a structured command, input files, expected outputs, and an
optional timeout. The caller chooses job granularity. The library does not parse
the consumer project's configuration or understand its experiment format.

The v1 implementation uses:

- **Ray on the host** for admission control, futures, and completion ordering;
- **Fabric/SSH** for remote control and live logs;
- **rsync over SSH** for project, input, and output transfer;
- **uv** for a locked Python dependency environment on each VM.

Ray never runs on the VMs. The Dispatcher owns an exclusive local Ray runtime;
this constraint is part of the v1 API contract.

### Driving use case

The first consumer is `../DFaasOptimizer`. A representative command is:

```text
python run.py --config config_files/eval_smoke.json --methods faas-madea --n_experiments 5
```

The project requires Python `>=3.10,<3.11`, has a fully pinned `uv.lock`, and
uses licensed Gurobi binaries. Its `pyproject.toml` declares
`[tool.uv] package = false`, so it can run directly from a source-tree copy
without installing the project itself.

## 2. Goals and non-goals

### Goals

- Provide a typed Python API consumable by other tools.
- Manage the lifecycle on SSH-only Debian/Ubuntu-like VMs: provision, dispatch,
  monitor, collect, terminate, and optionally purge.
- Respect a caller-declared concurrency limit for each VM.
- Isolate concurrent jobs and retry attempts from one another.
- Reproduce a locked dependency environment with explicitly pinned uv and
  Python versions.
- Keep job commands and file mappings generic.
- Make timeout, cancellation, host failure, and retry behaviour deterministic.
- Be testable end-to-end with `multipass-sdk`, without a runtime dependency on
  Multipass.

### Non-goals (v1)

- No autoscaling or dynamic VM provisioning.
- No native Ray cluster on the VMs.
- No in-memory data passing between jobs.
- No CLI.
- No non-Linux workers.
- No password-based SSH.
- No installation of the consumer project into the shared virtual environment.
  Dependencies are installed; the copied source tree must be directly
  executable. Installed console-script entry points from the consumer project
  are not supported.
- No resumable batches after the host process exits.
- No acquisition or validation of application-specific licenses.

## 3. Architecture decisions

### 3.1 Host-side Ray scheduler

The only required network path is host → VM over SSH. Each Ray task runs on the
host and controls the sequential attempts of one logical job. VMs do not need
Ray or direct connectivity to one another.

Ray supplies futures, admission control, task visibility, and completion
ordering. One Ray task represents one logical job and runs that job's attempts
sequentially. Ray does not own application retries: automatic Ray task retries
are disabled because a remote subprocess has side effects and cannot be safely
re-executed without cleanup and host reconciliation.

### 3.2 Exclusive Ray runtime

The Dispatcher owns one local Ray runtime with these invariants:

1. `Dispatcher.setup()` fails with `RayRuntimeConflictError` if
   `ray.is_initialized()` is already true. v1 does not attach to caller-owned or
   externally discovered Ray clusters.
2. It provisions the inventory first, freezes the healthy host set, and then
   starts Ray explicitly with `address="local"`, a unique namespace, and a
   custom resource `vm_slot = sum(host.slots for host in healthy_hosts)`.
3. Remote attempt tasks request `num_cpus=0`, `resources={"vm_slot": 1}`, and
   `max_retries=0`. The `HostLease` actor also declares `num_cpus=0` explicitly.
   Thus host CPU resources do not accidentally reduce VM-slot concurrency.
4. Only the Dispatcher that successfully started Ray may call `ray.shutdown()`.
5. A process can run only one Dispatcher at a time in v1.
6. Each VM is exclusively leased to one active Dispatcher session. Setup
   acquires a heartbeat-backed remote session lock before provisioning; a live
   lock from another session raises `HostInUseError`. This prevents independent
   host processes from exceeding the same VM's slot limit.

This exclusive-runtime rule avoids custom-resource conflicts and global Ray
lifecycle ambiguity. Supporting a caller-owned runtime is future work.

### 3.3 Backend-neutral public handles

Ray types are not exposed by the public API. A `JobHandle` is an opaque,
library-owned handle; `SshRayBackend` maps it internally to a Ray `ObjectRef`.
This keeps the API usable by a future backend without forcing consumers to
import Ray.

```python
class ExecutionBackend(ABC):
    def setup(self, inventory: Inventory, project: Project) -> ProvisioningReport: ...
    def submit(self, batch_id: str, job: Job) -> JobHandle: ...
    def status(self, handle: JobHandle) -> JobStatus: ...
    def cancel(self, handle: JobHandle) -> None: ...
    def resolve(self, handle: JobHandle) -> JobResult: ...
    def teardown(self, *, purge: bool = False) -> None: ...
```

### 3.4 Alternatives considered

- `ThreadPoolExecutor` or asyncio would implement the SSH work queue with less
  runtime machinery, but would remove the required Ray scheduling and dashboard
  integration.
- A native Ray cluster would use more of Ray's distributed features, but would
  require worker-to-worker ports and add Ray to the consumer environment.
- Static host assignment at submit time is simpler but leaves fast hosts idle
  while slow hosts retain queued work.

## 4. Public API and data model

### 4.1 Hosts and inventory

```python
@dataclass(frozen=True)
class RemoteHost:
    host: str
    user: str
    slots: int = 1
    port: int = 22
    identity_file: str | None = None
    known_hosts_file: str = "~/.ssh/known_hosts"

@dataclass(frozen=True)
class Inventory:
    hosts: tuple[RemoteHost, ...]

    @classmethod
    def from_yaml(cls, path: str) -> "Inventory": ...
```

Validation rejects empty inventories, duplicate `(host, port, user)` entries,
non-positive slots, missing identity files, and missing known-hosts files. Host
key checking is always enabled. Fabric and rsync receive the same resolved SSH
identity, port, user, and known-hosts settings.

### 4.2 Project and secrets

```python
@dataclass(frozen=True)
class SecretFile:
    source: str                  # local file
    remote_name: str             # one normalized filename, not a path
    env_var: str | None = None   # optional variable pointing at the remote file
    mode: int = 0o600

@dataclass(frozen=True)
class Project:
    path: str
    project_id: str              # stable, path-safe identifier
    python: str                  # exact version, for example "3.10.18"
    uv_version: str              # exact version, for example "0.11.25"
    secrets: tuple[SecretFile, ...] = ()
    exclude: tuple[str, ...] = (".venv/", ".git/", "solutions/")
    dependency_groups: tuple[str, ...] = ()
```

`python` and `uv_version` must be exact versions. Dependency groups are
explicit: the default installs runtime dependencies only. The core has no
knowledge of Gurobi; the caller expresses its license as, for example,
`SecretFile(..., remote_name="gurobi.lic", env_var="GRB_LICENSE_FILE")`.

Secrets are copied outside project and run directories to:

```text
~/.ray_dispatcher/secrets/<project_id>/<remote_name>
```

They are never included in digests, result files, or logs. Destination files
are owned by the SSH user and use the requested restrictive mode.

### 4.3 Inputs, outputs, and jobs

```python
@dataclass(frozen=True)
class InputSpec:
    source: str                  # absolute or Project.path-relative local path
    destination: str            # normalized run-root-relative POSIX path

@dataclass(frozen=True)
class OutputSpec:
    source: str                  # normalized run-root-relative POSIX path
    destination: str | None = None  # relative to the job's local outputs dir
    required: bool = True

@dataclass(frozen=True)
class Job:
    id: str
    command: tuple[str, ...]
    inputs: tuple[InputSpec, ...] = ()
    outputs: tuple[OutputSpec, ...] = ()
    env: Mapping[str, str] = field(default_factory=dict)
    timeout_s: float | None = None
    cwd: str = "."              # normalized run-root-relative POSIX path
```

`id` must match `[A-Za-z0-9][A-Za-z0-9._-]{0,127}` and be unique within a
batch. Commands cannot be empty or contain NUL bytes. Environment keys must be
valid POSIX variable names. `cwd`, input destinations, and output paths cannot
be absolute, contain `..`, or resolve through a symlink outside the run root.
Output destinations must also remain beneath the local job output directory.

An absolute input source is allowed because its remote destination is explicit;
there is no implicit mapping for absolute paths.

### 4.4 Status, handles, attempts, and results

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
    retry_on: frozenset[FailureKind] = frozenset({
        FailureKind.SSH,
        FailureKind.HOST_LOST,
        FailureKind.COLLECTION,
    })

@dataclass(frozen=True)
class JobHandle:
    batch_id: str
    job_id: str
    token: str                   # opaque; consumers must not interpret it

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

`RetryPolicy.max_attempts` must be at least one. Result durations are monotonic
elapsed time. `JobResult.returncode` and `host` describe the final attempt;
attempt-level details remain available in `attempts`.

### 4.5 Dispatcher

```python
class Dispatcher:
    def __init__(
        self,
        inventory: Inventory,
        project: Project,
        *,
        results_dir: str = "./results",
        retry_policy: RetryPolicy = RetryPolicy(),
        raise_on_failure: bool = False,
        require_all_hosts: bool = True,
        backend: ExecutionBackend | None = None,
    ): ...

    def setup(self, *, force: bool = False) -> ProvisioningReport: ...
    def submit(self, jobs: Sequence[Job], *, batch_id: str | None = None) -> list[JobHandle]: ...
    def status(self, handle: JobHandle) -> JobStatus: ...
    def cancel(self, handle: JobHandle) -> None: ...
    def as_completed(self, handles: Sequence[JobHandle]) -> Iterator[JobResult]: ...
    def run(self, jobs: Sequence[Job], *, batch_id: str | None = None) -> list[JobResult]: ...
    def teardown(self, *, purge: bool = False) -> None: ...
    def __enter__(self) -> "Dispatcher": ...
    def __exit__(self, *exc: object) -> None: ...
```

`setup()` validates and provisions hosts before starting Ray. `submit()` calls
it automatically if needed. Once the runtime has started, `setup()` is an
idempotent no-op and `setup(force=True)` is rejected; force-reprovisioning
requires teardown and a new Dispatcher so Ray resource capacity cannot diverge
from the healthy inventory. `batch_id` is generated as a UUID when omitted and
obeys the same path rules as job IDs. Reusing an existing local batch directory
fails with `BatchExistsError`; implicit overwrite and resume are forbidden.

`as_completed()` yields completion order. `run()` drains every job and returns
results in input order. If `raise_on_failure=True`, it still drains the batch and
then raises `BatchFailedError(results)`.

On context exit, outstanding jobs are cancelled and reconciled before Ray is
shut down. `teardown(purge=True)` is rejected while jobs remain active.
`__enter__()` itself performs no network operation; setup remains explicit or
is triggered by the first submission.

### Usage sketch

```python
from ray_dispatcher import (
    Dispatcher,
    InputSpec,
    Inventory,
    Job,
    OutputSpec,
    Project,
    RemoteHost,
    SecretFile,
)

inventory = Inventory((
    RemoteHost("10.0.0.11", user="ubuntu", slots=2),
    RemoteHost("10.0.0.12", user="ubuntu", slots=2),
))

project = Project(
    path="../DFaasOptimizer",
    project_id="dfaas-optimizer",
    python="3.10.18",
    uv_version="0.11.25",
    secrets=(SecretFile(
        source="~/lic/gurobi.lic",
        remote_name="gurobi.lic",
        env_var="GRB_LICENSE_FILE",
    ),),
)

jobs = [Job(
    id="madea-smoke",
    command=("python", "run.py", "--config", "config_files/eval_smoke.json",
             "--methods", "faas-madea", "--n_experiments", "5"),
    inputs=(InputSpec("config_files/eval_smoke.json", "config_files/eval_smoke.json"),),
    outputs=(OutputSpec("solutions/eval_smoke", required=True),),
)]

with Dispatcher(inventory, project, results_dir="./results") as dispatcher:
    results = dispatcher.run(jobs)
```

## 5. Module breakdown

```text
src/ray_dispatcher/
├── models.py             # validated public value objects
├── errors.py             # public exception hierarchy
├── paths.py              # normalization and containment checks
├── ssh.py                # Fabric + rsync transport and shared SSH options
├── remote_runner.py      # versioned remote subprocess supervisor
├── provisioning.py      # manifests, uv/Python, project sync, secrets
├── scheduling.py        # Ray task, HostLease actor, status registry
├── results.py            # attempt layout, collection, result manifest
├── backends/
│   ├── base.py           # ExecutionBackend
│   └── ssh_ray.py        # exclusive local-Ray backend
├── dispatcher.py         # public lifecycle and batch orchestration
└── __init__.py           # public exports
```

Only `ssh.py` constructs Fabric connections or rsync invocations. Only
`remote_runner.py` creates the remote application subprocess. Only
`SshRayBackend` imports Ray-facing implementation types.

## 6. Provisioning and cache invalidation

### 6.1 Remote layout

```text
~/.ray_dispatcher/
├── bin/<runner_digest>/remote_runner.py
├── projects/<project_id>/
│   ├── source/
│   ├── source-manifest.json
│   └── envs/<environment_digest>/
│       ├── .venv/
│       └── environment-manifest.json
├── secrets/<project_id>/...
└── runs/<batch_id>/<job_id>/<attempt>/...
```

`project_id` is stable. It is not a content hash.

### 6.2 Digests

- `source_digest` covers all transferred source files after excludes, including
  file paths, modes, symlink targets, and contents.
- `environment_digest` covers `pyproject.toml`, `uv.lock`, exact Python version,
  exact uv version, dependency groups, sync flags, and worker platform.
- `runner_digest` covers the bundled remote supervisor.

Secrets are intentionally excluded from all digests.

### 6.3 Provisioning algorithm

For every host, in parallel but bounded by inventory size:

1. Verify SSH host key and authentication, then atomically acquire the remote
   Dispatcher session lock. A stale lock can be recovered only after its
   heartbeat has expired and reconciliation finds no live runner owned by that
   session. A host-side heartbeat keeps the lock live throughout provisioning
   and execution.
2. Verify required disk space, `python3`, and `rsync`.
3. Install the exact uv version from its versioned official installer URL into
   a dispatcher-owned, version-specific path. Never replace or depend on a
   system-wide `uv`. Verify the installed binary's version before use.
4. Run `uv python install <exact-version>` and verify the resolved interpreter
   version.
5. Rsync source into a staging directory with `--delete` and configured
   excludes, then atomically replace `source/`. Write `source-manifest.json`
   only after the sync succeeds.
6. If the environment manifest for `environment_digest` is absent or invalid,
   create it in staging, set `UV_PROJECT_ENVIRONMENT` to the staging `.venv`,
   and run the equivalent of:

   ```text
   uv sync --project <source> --locked --no-install-project \
     --no-install-workspace \
     --no-default-groups \
     --python <exact-version>
   ```

   Explicit dependency groups add their corresponding `--group` arguments.
   Atomically publish the environment only after sync and interpreter smoke
   checks succeed. Published environments are logically immutable: dispatcher
   operations never modify them in place.
7. Install the versioned remote runner.
8. Copy secrets with their declared modes and verify ownership without printing
   secret contents.

`force=True` repeats validation and transfer but does not weaken atomicity.
Stale source snapshots and environments remain available until
`teardown(purge=True)`; normal setup never deletes an environment in use.

`require_all_hosts=True` makes any provisioning failure abort setup after
collecting a complete `ProvisioningReport`. When false, failed hosts are marked
unavailable and execution may proceed only if at least one host is healthy.
Setup releases every lock acquired for a failed or aborted provisioning run.

## 7. Attempt execution

Each attempt follows this protocol:

1. The Ray task acquires one global `vm_slot` and a concrete lease token from
   the asynchronous `HostLease` actor. Its `acquire()` waits on an
   `asyncio.Condition`, allowing release, heartbeat, and reconciliation calls to
   execute while jobs wait; no actor method blocks the actor event loop. A retry
   passes previously attempted hosts as exclusions; a host is reused only after
   every healthy host has been tried.
2. Create a fresh remote directory:
   `runs/<batch_id>/<job_id>/<attempt>/`. Existing directories are an error.
3. Copy the provisioned source tree locally on the VM using rsync. The dependency
   environment is not copied; `.venv` is a symlink to the logically immutable
   environment selected by `environment_digest`.
4. Push each input to its explicit destination after containment and symlink
   checks.
5. Write a JSON runner manifest containing argv, cwd, environment additions,
   log paths, and secret environment mappings. No shell command is assembled
   from user strings.
6. Invoke the versioned remote runner. It uses `subprocess.Popen(argv, cwd=...,
   env=..., start_new_session=True)`, records PID and process-group ID, and
   forwards stdout/stderr over SSH while also keeping remote copies.
7. The host writes streamed logs to the attempt result directory. Binary or
   undecodable output is preserved with replacement markers; log streaming
   failure does not hide the remote exit status.
8. After any normal process exit, best-effort collect declared outputs into
   attempt-scoped staging, including partial outputs from failed commands. For
   an otherwise successful command, a missing required output produces
   `OUTPUT_MISSING`; a command failure remains the primary failure kind. Missing
   optional outputs are recorded in the attempt manifest.
9. Atomically publish the successful attempt's outputs as the job's final
   `outputs/` directory and write `result.json`.
10. Release the lease only after the remote process is confirmed terminated and
    output collection has finished.

The runner prepends the shared `.venv/bin` to `PATH`, sets `VIRTUAL_ENV`, and
sets declared secret variables. It never invokes `uv run`, which could mutate or
resynchronize the shared environment.

## 8. Timeout, cancellation, leases, and host loss

### 8.1 Process termination

On timeout or cancellation, the host opens a control SSH connection, sends
`SIGTERM` to the recorded remote process group, waits a bounded grace period,
then sends `SIGKILL`. The attempt is complete only after the runner or a remote
probe confirms the process group no longer exists.

Closing the original SSH channel is never treated as process termination.

### 8.2 Lease safety

Every lease contains a random token, host, slot, attempt identity, expiry, and
heartbeat timestamp. The attempt task heartbeats while the remote process runs.
Release is token-checked and idempotent.

If a task or SSH connection disappears:

1. `HostLease` expires the lease after its heartbeat deadline.
2. The host is quarantined rather than immediately returned to the pool.
3. A reconciliation probe reads the runner state and terminates any orphaned
   process group.
4. The host becomes healthy only after reconciliation succeeds.

When no healthy capacity remains, pending work fails with `NoHealthyHostsError`;
it does not wait forever while occupying Ray resources.

### 8.3 Retry policy

The default `RetryPolicy` retries SSH, host-loss, and collection failures for at
most two total attempts. Command failures, missing outputs, and timeouts are not
retried by default because they are commonly deterministic. Callers may opt into
them. The same logical-job Ray task starts its next attempt only after
termination or quarantine has made the previous attempt safe. Ray itself always
uses `max_retries=0`.

## 9. Results, monitoring, and errors

### 9.1 Local layout

```text
<results_dir>/<batch_id>/<job_id>/
├── attempts/
│   ├── 1/stdout.log
│   ├── 1/stderr.log
│   ├── 1/attempt.json
│   └── 2/...
├── outputs/
└── result.json
```

Attempts never overwrite one another. Final outputs come from exactly one
successful attempt. Failed attempts remain available for diagnosis.

### 9.2 Observable status

The backend maintains a status registry keyed by `JobHandle`. `status()` exposes
`PENDING` and `RUNNING`; `as_completed()` yields terminal results. An optional
`progress` extra renders the same registry with Rich. Ray's dashboard URL is
reported through logging, not printed unconditionally.

### 9.3 Error contract

- Model and path errors are raised synchronously before submission.
- Setup failures raise `ProvisioningError(report)`.
- Individual job failures become `JobResult` values.
- `raise_on_failure=True` raises only after all submitted jobs reach a terminal
  state and attaches the ordered results.
- A backend-wide failure that prevents result construction raises
  `DispatcherError` and triggers cancellation/reconciliation of outstanding
  attempts.
- Cleanup errors are aggregated and never replace an exception already escaping
  the context manager; they are attached as notes and logged.

## 10. Teardown and persistence

Normal teardown:

1. cancel and reconcile outstanding attempts;
2. stop status and lease actors;
3. if requested, purge only this project's inactive remote state;
4. release remote Dispatcher session locks;
5. call `ray.shutdown()` only for the owned runtime;
6. otherwise retain remote sources, environments, secrets, and failed run
   directories.

`purge=True` additionally removes this project's source, environments, secrets,
and completed run directories after confirming that no process uses them. It
does not delete unrelated project IDs. Failed remote deletion is reported per
host.

Successful remote run directories are deleted after collection by default.
Failed, timed-out, or cancelled attempts are retained unless purged.

## 11. Testing strategy

### Unit tests without VMs

Inject fake SSH, rsync, clock, and Ray-adapter interfaces. Cover:

- all model and path validations, including traversal and symlink escape;
- source/environment digest changes and atomic manifest publication;
- exact provisioning commands and dependency-group selection;
- secret permissions and log redaction;
- lease capacity, token validation, expiry, quarantine, and reconciliation;
- default Ray options (`num_cpus=0`, one `vm_slot`, `max_retries=0`);
- retry classification and different-host preference;
- result ordering, batch collisions, required/optional outputs, and
  `raise_on_failure`;
- cancellation and teardown ownership;
- remote session-lock conflict, expiry, and safe recovery.

### Local Ray integration tests

Start an isolated local Ray runtime and assert:

- concurrency reaches `sum(slots)` even when it exceeds host logical CPUs;
- per-host concurrency never exceeds its declared slots;
- task failure does not cause an automatic Ray retry;
- a pre-initialized Ray runtime is rejected without being shut down;
- completion order and status transitions are observable.

### Multipass end-to-end tests

Opt-in tests use `multipass-sdk` only as a VM factory:

1. launch N Ubuntu VMs with cloud-init installing `rsync` and injecting the
   test public key;
2. wait for `cloud-init status --wait`, then for port 22;
3. collect each VM's SSH host key into a test-owned temporary `known_hosts`
   file and reference it from `RemoteHost`;
4. provision a synthetic `[tool.uv] package = false` project;
5. run concurrent jobs and verify logs, outputs, manifests, and slot limits;
6. exercise timeout termination, retry on another VM, host quarantine, missing
   output, source-only change, lockfile change, and cached second setup;
7. delete and purge all fixture VMs in `finally` blocks.

DFaasOptimizer remains a manual smoke target because it is heavy and requires a
Gurobi license.

## 12. Packaging

- `src/` layout with `tests/unit`, `tests/integration`, `tests/e2e`, and
  `examples`.
- `pyproject.toml` declares `requires-python = ">=3.10"`.
- Runtime dependencies, including Ray and Fabric, use exact tested pins in the
  lockfile and bounded compatible constraints in published metadata.
- Rich is optional under `ray-dispatcher[progress]`.
- `multipass-sdk` is a development/test dependency only.
- The project is managed with uv. CI runs formatting, linting, typing, unit
  tests, and local-Ray integration tests; Multipass tests are opt-in.

## 13. Assumptions and constraints

- The host can authenticate to every VM with a key or SSH agent.
- Each VM host key is present in the configured known-hosts file.
- A VM is assigned to only one active Dispatcher session; this is enforced with
  the remote session lock.
- Workers are Debian/Ubuntu-like Linux systems with `python3`, POSIX signals,
  and outbound internet during initial provisioning.
- rsync is installed on host and workers.
- The SSH user can write beneath `~/.ray_dispatcher` and signal its own
  processes.
- The host has enough local capacity to run Ray and one lightweight worker
  process per active VM slot.
- Each VM has sufficient disk for one source copy per active attempt plus
  outputs and cached dependency environments.
- The consumer project runs correctly from a source-tree copy with dependencies
  available in `.venv`; it does not require installation of itself.
- Jobs are trusted code running as the SSH user. They do not intentionally
  mutate shared environments, read other project secrets, daemonize, or escape
  the process group created by the remote runner.
- Application licenses and their VM eligibility remain the caller's
  responsibility.

## 14. Future work

- caller-owned or externally managed Ray runtime support;
- `NativeRayClusterBackend`;
- installed-project and console-entry-point execution modes;
- resumable batches backed by persistent state;
- thin CLI;
- offline provisioning and shared wheel caches;
- shared filesystem support for large datasets;
- dynamic inventory and autoscaling.
