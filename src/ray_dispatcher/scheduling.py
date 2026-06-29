"""Scheduling state machine: per-host slot leases with quarantine (spec §7.1, §8.2).

`LeasePool` is a pure, single-threaded, clock-injected state machine — no Ray,
no SSH. Phase 4b wraps it in the async HostLease Ray actor and adds the SSH
reconciliation probe.
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

from .errors import DispatcherError, ModelValidationError, NoHealthyHostsError
from .models import AttemptResult, FailureKind, Job, JobStatus, Project
from .provisioning import RemoteLayout, RunPaths
from .results import (
    JobLayout,
    collect_outputs,
    create_attempt_dir,
    publish_job_outputs,
    write_attempt_json,
)
from .ssh import CommandResult, Transport, terminate_process_group, write_remote_file


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
