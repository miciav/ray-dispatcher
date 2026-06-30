"""Exclusive local-Ray execution backend (spec §3.2, §3.3).

This is the only module that imports Ray. The HostLease actor wraps the Ray-free
LeaseService; SshRayBackend owns one local Ray runtime, started after provisioning
and shut down at teardown.
"""

from __future__ import annotations

import secrets
import threading
import uuid
from collections.abc import Callable, Iterable

import ray

from ..digests import runner_digest
from ..errors import RayRuntimeConflictError
from ..models import (
    HostProvisioningResult,
    Inventory,
    Job,
    JobHandle,
    JobResult,
    JobStatus,
    Project,
    ProvisioningReport,
    RemoteHost,
    RetryPolicy,
)
from ..provisioning import ProvisioningOutcome, RemoteLayout, _default_transport, provision
from ..results import JobLayout, write_result_json
from ..scheduling import HostRuntime, Lease, LeaseService, run_job, secret_env_map
from ..ssh import Transport
from .base import ExecutionBackend

# The async lease state machine, run as a Ray actor that holds no host CPU (§3.2.3).
HostLease = ray.remote(num_cpus=0)(LeaseService)


class _ActorLeaseHandle:
    """Synchronous LeaseHandle with heartbeat daemon (§8.2)."""

    def __init__(
        self,
        actor: ray.actor.ActorHandle[LeaseService],
        *,
        heartbeat_interval_s: float = 5.0,
    ) -> None:
        self._actor = actor
        self._heartbeat_interval_s = heartbeat_interval_s
        self._stop = threading.Event()
        self._hb_thread: threading.Thread | None = None

    def acquire(self, attempt_id: str, *, exclude: Iterable[str] = ()) -> Lease:
        lease = ray.get(self._actor.acquire.remote(attempt_id, exclude=tuple(exclude)))
        self._stop.clear()
        self._hb_thread = threading.Thread(
            target=self._heartbeat, args=(lease.token,), daemon=True, name="lease-heartbeat"
        )
        self._hb_thread.start()
        return lease  # type: ignore[no-any-return]

    def release(self, token: str) -> None:
        self._stop.set()
        if self._hb_thread is not None:
            self._hb_thread.join(timeout=self._heartbeat_interval_s + 1.0)
            self._hb_thread = None
        ray.get(self._actor.release.remote(token))

    def _heartbeat(self, token: str) -> None:
        while not self._stop.wait(self._heartbeat_interval_s):
            try:
                alive = ray.get(self._actor.heartbeat.remote(token))
            except Exception:
                break
            if not alive:
                break


@ray.remote(num_cpus=0, resources={"vm_slot": 1}, max_retries=0)
def _attempt_task(
    job: Job,
    batch_id: str,
    local: JobLayout,
    runtimes: dict[str, HostRuntime],
    inv_hosts: dict[str, RemoteHost],
    actor: ray.actor.ActorHandle,  # type: ignore[type-arg]
    transport_factory: Callable[[RemoteHost], Transport],
    policy: RetryPolicy,
    heartbeat_interval_s: float = 5.0,
) -> JobResult:
    """Ray task: run one logical job; terminates as a JobResult (§3.2.3, §6a residual)."""
    lease_handle = _ActorLeaseHandle(actor, heartbeat_interval_s=heartbeat_interval_s)
    try:
        result = run_job(
            job,
            batch_id=batch_id,
            lease=lease_handle,
            runtime_for=runtimes.__getitem__,
            transport_for=lambda host: transport_factory(inv_hosts[host]),
            local=local,
            policy=policy,
        )
        write_result_json(local.result_json, result)
        return result
    except ray.exceptions.TaskCancelledError:
        cancelled = JobResult(
            id=job.id, batch_id=batch_id, status=JobStatus.CANCELLED,
            returncode=None, duration_s=0.0, host=None, output_dir=None,
            attempts=(), error="cancelled",
        )
        write_result_json(local.result_json, cancelled)
        return cancelled
    except Exception as exc:
        internal = JobResult(
            id=job.id, batch_id=batch_id, status=JobStatus.FAILED,
            returncode=None, duration_s=0.0, host=None, output_dir=None,
            attempts=(), error=f"INTERNAL: {type(exc).__name__}: {exc}",
        )
        write_result_json(local.result_json, internal)
        return internal


class SshRayBackend(ExecutionBackend):
    """Owns one exclusive local Ray runtime and a HostLease actor (spec §3.2)."""

    def __init__(
        self,
        *,
        runner_path: str | None = None,
        transport_factory: Callable[..., Transport] | None = None,
        retry_policy: RetryPolicy = RetryPolicy(),  # noqa: B008
        min_disk_mb: int = 500,
        results_dir: str = "./results",
    ) -> None:
        from .. import remote_runner

        self._runner_path = runner_path or remote_runner.__file__
        self._transport_factory = transport_factory or _default_transport
        self._retry_policy = retry_policy
        self._min_disk_mb = min_disk_mb
        self._results_dir = results_dir
        self._owns_runtime = False
        self._actor: ray.actor.ActorHandle | None = None  # type: ignore[type-arg]
        self._outcome: ProvisioningOutcome | None = None
        self._runtimes: dict[str, HostRuntime] = {}
        self._inv_hosts: dict[str, RemoteHost] = {}
        self._refs: dict[str, object] = {}

    def setup(self, inventory: Inventory, project: Project) -> ProvisioningReport:
        if ray.is_initialized():
            raise RayRuntimeConflictError(
                "a Ray runtime is already initialized; ray_dispatcher owns its own "
                "local runtime and will not attach to an external one (§3.2)"
            )
        # Provision first; provision() raises NoHealthyHostsError if none succeed,
        # so Ray is never started for an empty healthy set (§3.2.2).
        outcome = provision(
            inventory, project,
            runner_path=self._runner_path, transport_factory=self._transport_factory,
            min_disk_mb=self._min_disk_mb,
        )
        self._outcome = outcome
        runner_dig = runner_digest(self._runner_path)
        healthy = {h.host: h for h in outcome.report.hosts if h.succeeded}
        inv_hosts = {h.host: h for h in inventory.hosts}
        inv_slots = {name: inv_hosts[name].slots for name in healthy}
        slots = inv_slots  # healthy host -> its slots
        self._inv_hosts = inv_hosts
        self._runtimes = {
            host_name: self._build_runtime(project, result, inv_hosts[host_name], runner_dig)
            for host_name, result in healthy.items()
        }
        ray.init(
            address="local",
            namespace=f"ray-dispatcher-{secrets.token_hex(8)}",  # unique per session (§3.2.2)
            resources={"vm_slot": float(sum(slots.values()))},
            # Exclude pyproject.toml / lockfiles from the working_dir package Ray auto-uploads
            # so workers don't attempt `uv sync` with relative editable paths that only
            # resolve at the caller's site (e.g. `editable+../../` inside examples/).
            runtime_env={"excludes": ["pyproject.toml", "uv.lock", "*.lock"]},
        )
        self._owns_runtime = True
        self._actor = HostLease.remote(slots)  # type: ignore[assignment]
        return outcome.report

    def _build_runtime(
        self, project: Project, result: HostProvisioningResult, host: RemoteHost, runner_dig: str
    ) -> HostRuntime:
        host_name = result.host
        transport = self._transport_factory(host)
        home = transport.run(["sh", "-c", 'printf %s "$HOME"']).stdout.strip()
        layout = RemoteLayout(home, project.project_id)
        env_dig = result.environment_digest
        assert env_dig is not None  # healthy hosts always have an environment digest
        return HostRuntime(
            host=host_name,
            layout=layout,
            environment_digest=env_dig,
            runner_digest=runner_dig,
            project_path=project.path,
            secret_env=secret_env_map(project, layout),
        )

    def submit(self, batch_id: str, job: Job) -> JobHandle:
        token = uuid.uuid4().hex
        local = JobLayout(self._results_dir, batch_id, job.id)
        ref = _attempt_task.remote(
            job, batch_id, local, self._runtimes, self._inv_hosts,
            self._actor, self._transport_factory, self._retry_policy,
        )
        self._refs[token] = ref
        return JobHandle(batch_id=batch_id, job_id=job.id, token=token)

    def status(self, handle: JobHandle) -> JobStatus:
        ref = self._refs.get(handle.token)
        if ref is None:
            return JobStatus.PENDING
        ready, _ = ray.wait([ref], timeout=0)
        if ready:
            result: JobResult = ray.get(ref)  # type: ignore[call-overload]
            return result.status
        return JobStatus.RUNNING

    def cancel(self, handle: JobHandle) -> None:
        ref = self._refs.get(handle.token)
        if ref is not None:
            ray.cancel(ref)

    def resolve(self, handle: JobHandle) -> JobResult:
        ref = self._refs[handle.token]
        try:
            return ray.get(ref)  # type: ignore[call-overload, no-any-return]
        except ray.exceptions.TaskCancelledError:
            return JobResult(
                id=handle.job_id, batch_id=handle.batch_id, status=JobStatus.CANCELLED,
                returncode=None, duration_s=0.0, host=None, output_dir=None,
                attempts=(), error="cancelled",
            )

    def running_hosts(self) -> dict[str, str]:
        if self._actor is None:
            return {}
        return ray.get(self._actor.current_hosts.remote())

    def teardown(self, *, purge: bool = False) -> None:
        # ponytail: purge + cancel/reconcile of outstanding attempts land in 6c/6d;
        # this slice does the runtime + lock release (§10.2,4,5).
        if self._actor is not None:
            ray.kill(self._actor)
            self._actor = None
        if self._outcome is not None:
            self._outcome.release_all()
        if self._owns_runtime:
            ray.shutdown()
            self._owns_runtime = False
