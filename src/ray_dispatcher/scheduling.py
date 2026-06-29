"""Scheduling and attempt execution (spec §7, §8.2).

Two concerns live here, both Ray-free so Phase 6 can wrap them:

- The lease state machine: `LeasePool` (pure, single-threaded, clock-injected —
  no Ray, no SSH), the async `LeaseService` wrapper, and the `reconcile_host`
  SSH probe (§7.1, §8.2).
- The host-side attempt driver: `execute_attempt` and its helpers
  (`secret_env_map`, `HostRuntime`, `build_runner_manifest`), which run one job
  attempt on one provisioned host over SSH (§7.2–7.9).

Deferred to Phase 6: the Ray task / actor decoration, the retry loop, timeout +
process termination (§8.1), and `JobResult` assembly. In particular
`execute_attempt` blocks for the whole job on one SSH call, so the Phase 6
wrapper must heartbeat the lease concurrently (§7.1/§8.2) — this driver cannot
beat while blocked.
"""

from __future__ import annotations

import asyncio
import json
import os
import posixpath
import secrets
import shlex
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from typing import Protocol

from .errors import DispatcherError, ModelValidationError, NoHealthyHostsError
from .models import AttemptResult, FailureKind, Job, JobResult, JobStatus, Project, RetryPolicy
from .provisioning import RemoteLayout, RunPaths
from .results import (
    JobLayout,
    collect_outputs,
    create_attempt_dir,
    publish_job_outputs,
    write_attempt_json,
)
from .ssh import (
    CommandResult,
    Transport,
    TransportError,
    terminate_process_group,
    write_remote_file,
)


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

    def release(self, token: str) -> bool:
        lease = self._leases.pop(token, None)
        if lease is None:
            return False
        self._used[lease.host].discard(lease.slot)
        return True

    def heartbeat(self, token: str) -> bool:
        lease = self._leases.get(token)
        if lease is None:
            return False
        now = self._now()
        if now >= lease.expiry_s:
            return False  # past deadline (even if not yet swept) -> logically dead
        self._leases[token] = replace(lease, heartbeat_s=now, expiry_s=now + self._ttl)
        return True

    def quarantine(self, host: str) -> None:
        self._quarantined.add(host)
        for token in [t for t, ls in self._leases.items() if ls.host == host]:
            lease = self._leases.pop(token)
            self._used[lease.host].discard(lease.slot)

    def mark_reconciled(self, host: str) -> None:
        self._quarantined.discard(host)

    def sweep_expired(self) -> list[str]:
        now = self._now()
        affected = {ls.host for ls in self._leases.values() if ls.expiry_s <= now}
        for host in affected:
            self.quarantine(host)
        return sorted(affected)

    def quarantined_hosts(self) -> list[str]:
        return sorted(self._quarantined)


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
    if pgid <= 1:
        return False  # 0 = caller's own group, 1 = init; never a runner pgid -> keep quarantined
    return terminate_process_group(transport, pgid, grace_s=grace_s)


# --- attempt driver (§7.2-7.9) -------------------------------------------------


def secret_env_map(project: Project, layout: RemoteLayout) -> dict[str, str]:
    """Map each declared secret's env var to its absolute remote path (spec §4.2).

    Secrets without an ``env_var`` are provisioned but not exported into the job.
    """
    return {
        s.env_var: f"{layout.secrets}/{s.remote_name}"
        for s in project.secrets
        if s.env_var is not None
    }


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
    venv: str,
    run: RunPaths,
    secret_env: Mapping[str, str],
) -> dict[str, object]:
    """Build the JSON manifest consumed by remote_runner.py (spec §7.5).

    The job argv and env travel as data here; remote_runner Popens argv with no
    shell, prepends venv_bin to PATH, sets VIRTUAL_ENV, and exports secret_env.
    ``cwd`` is derived from ``run.run_root`` so it can never disagree with where
    the source is rsync'd; ``venv`` is the immutable env (distinct from the
    ``run.venv`` symlink that points at it).
    """
    return {
        "argv": list(job.command),
        "cwd": posixpath.normpath(f"{run.run_root}/{job.cwd}"),
        "env": dict(job.env),
        "secret_env": dict(secret_env),
        "venv_bin": f"{venv}/bin",
        "virtual_env": venv,
        "stdout_path": run.stdout,
        "stderr_path": run.stderr,
        "pid_path": run.pid,
        "result_path": run.result,
    }


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
    # InputSpec.destination is lexically validated (no absolute/`..`) at the model
    # boundary; remote symlink-escape resolution is not done here — acceptable under
    # the §13 trusted-job / trusted-source-tree model.
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
    manifest = build_runner_manifest(job, venv=venv, run=run, secret_env=runtime.secret_env)
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


def should_retry(policy: RetryPolicy, kind: FailureKind | None, completed_attempts: int) -> bool:
    """Decide whether to make another attempt after a failure (spec §8.3).

    Retries only a configured-retryable failure kind, and only while attempts
    remain. Success and non-retryable kinds (COMMAND/OUTPUT_MISSING/TIMEOUT by
    default) stop immediately.
    """
    if kind is None or kind not in policy.retry_on:
        return False
    return completed_attempts < policy.max_attempts


def assemble_job_result(
    job_id: str, batch_id: str, attempts: list[AttemptResult], *, outputs_dir: str
) -> JobResult:
    """Fold the attempt history into the job's final result (spec §4.4).

    The final attempt describes returncode/host/error; duration is the total
    across attempts; output_dir is set only when the final attempt succeeded.
    """
    final = attempts[-1]
    return JobResult(
        id=job_id,
        batch_id=batch_id,
        status=final.status,
        returncode=final.returncode,
        duration_s=sum(a.duration_s for a in attempts),
        host=final.host,
        output_dir=outputs_dir if final.status is JobStatus.SUCCEEDED else None,
        attempts=tuple(attempts),
        error=final.error,
    )


class LeaseHandle(Protocol):
    """The slice of the lease actor that run_job needs (6b adapts the Ray actor)."""

    def acquire(self, attempt_id: str, *, exclude: Iterable[str] = ()) -> Lease: ...

    def release(self, token: str) -> None: ...


def _failed_attempt(
    n: int, host: str, kind: FailureKind, error: str, local: JobLayout
) -> AttemptResult:
    """Build the AttemptResult for an attempt that raised before producing one."""
    return AttemptResult(
        number=n,
        host=host,
        status=JobStatus.FAILED,
        returncode=None,
        duration_s=0.0,
        stdout_log=str(local.stdout_log(n)),
        stderr_log=str(local.stderr_log(n)),
        failure_kind=kind,
        error=error,
    )


def run_job(
    job: Job,
    *,
    batch_id: str,
    lease: LeaseHandle,
    runtime_for: Callable[[str], HostRuntime],
    transport_for: Callable[[str], Transport],
    local: JobLayout,
    policy: RetryPolicy,
) -> JobResult:
    """Run one logical job to a terminal JobResult, retrying per policy (spec §8.3).

    Each attempt leases a host (excluding already-tried hosts so retries prefer a
    fresh VM, §7.1), runs execute_attempt, and classifies the outcome. A raised
    TransportError becomes an SSH failure and a DispatcherError an INTERNAL
    failure (both surfaced as AttemptResults). NoHealthyHostsError from acquire
    propagates. The Phase 6b Ray task wraps this with heartbeat + timeout.
    """
    attempts: list[AttemptResult] = []
    tried: set[str] = set()
    while True:
        n = len(attempts) + 1
        leased = lease.acquire(job.id, exclude=tried)
        try:
            result = execute_attempt(
                transport_for(leased.host), runtime_for(leased.host), job,
                batch_id=batch_id, attempt=n, local=local,
            )
        except TransportError as e:
            result = _failed_attempt(n, leased.host, FailureKind.SSH, str(e), local)
        except DispatcherError as e:
            result = _failed_attempt(n, leased.host, FailureKind.INTERNAL, str(e), local)
        finally:
            lease.release(leased.token)
        attempts.append(result)
        tried.add(leased.host)
        if result.status is JobStatus.SUCCEEDED or not should_retry(
            policy, result.failure_kind, len(attempts)
        ):
            break
    return assemble_job_result(
        job.id, batch_id, attempts, outputs_dir=str(local.outputs_dir)
    )


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
                if self._pool.healthy_host_count() == 0:
                    raise NoHealthyHostsError("no healthy hosts remain")
                await self._cond.wait()

    async def release(self, token: str) -> None:
        async with self._cond:
            self._pool.release(token)
            self._cond.notify_all()

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
