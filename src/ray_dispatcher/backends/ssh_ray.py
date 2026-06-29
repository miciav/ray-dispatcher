"""Exclusive local-Ray execution backend (spec §3.2, §3.3).

This is the only module that imports Ray. The HostLease actor wraps the Ray-free
LeaseService; SshRayBackend owns one local Ray runtime, started after provisioning
and shut down at teardown.
"""

from __future__ import annotations

import secrets
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
from ..scheduling import HostRuntime, Lease, LeaseService, secret_env_map
from ..ssh import Transport
from .base import ExecutionBackend

# The async lease state machine, run as a Ray actor that holds no host CPU (§3.2.3).
HostLease = ray.remote(num_cpus=0)(LeaseService)


class _ActorLeaseHandle:
    """Synchronous LeaseHandle over the async HostLease actor (used by the 6c task)."""

    def __init__(self, actor: ray.actor.ActorHandle[LeaseService]) -> None:
        self._actor = actor

    def acquire(self, attempt_id: str, *, exclude: Iterable[str] = ()) -> Lease:
        return ray.get(self._actor.acquire.remote(attempt_id, exclude=tuple(exclude)))  # type: ignore[no-any-return]

    def release(self, token: str) -> None:
        ray.get(self._actor.release.remote(token))


class SshRayBackend(ExecutionBackend):
    """Owns one exclusive local Ray runtime and a HostLease actor (spec §3.2)."""

    def __init__(
        self,
        *,
        runner_path: str | None = None,
        transport_factory: Callable[..., Transport] | None = None,
        retry_policy: RetryPolicy = RetryPolicy(),  # noqa: B008
        min_disk_mb: int = 500,
    ) -> None:
        from .. import remote_runner

        self._runner_path = runner_path or remote_runner.__file__
        self._transport_factory = transport_factory or _default_transport
        self._retry_policy = retry_policy
        self._min_disk_mb = min_disk_mb
        self._owns_runtime = False
        self._actor: ray.actor.ActorHandle | None = None  # type: ignore[type-arg]
        self._outcome: ProvisioningOutcome | None = None
        self._runtimes: dict[str, HostRuntime] = {}

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
        self._runtimes = {
            host_name: self._build_runtime(project, result, inv_hosts[host_name], runner_dig)
            for host_name, result in healthy.items()
        }
        ray.init(
            address="local",
            namespace=f"ray-dispatcher-{secrets.token_hex(8)}",  # unique per session (§3.2.2)
            resources={"vm_slot": float(sum(slots.values()))},
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

    def _teardown_runtime_for_test(self) -> None:
        """Minimal runtime release for tests written before Task 5's teardown."""
        if self._actor is not None:
            ray.kill(self._actor)
            self._actor = None
        if self._outcome is not None:
            self._outcome.release_all()
        if self._owns_runtime:
            ray.shutdown()
            self._owns_runtime = False

    def submit(self, batch_id: str, job: Job) -> JobHandle:
        raise NotImplementedError  # Phase 6c

    def status(self, handle: JobHandle) -> JobStatus:
        raise NotImplementedError  # Phase 6c

    def cancel(self, handle: JobHandle) -> None:
        raise NotImplementedError  # Phase 6c

    def resolve(self, handle: JobHandle) -> JobResult:
        raise NotImplementedError  # Phase 6c

    def teardown(self, *, purge: bool = False) -> None:
        raise NotImplementedError  # Task 5
