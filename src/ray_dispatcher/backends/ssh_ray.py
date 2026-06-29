"""Exclusive local-Ray execution backend (spec §3.2, §3.3).

This is the only module that imports Ray. The HostLease actor wraps the Ray-free
LeaseService; SshRayBackend owns one local Ray runtime, started after provisioning
and shut down at teardown.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable

import ray

from ..errors import RayRuntimeConflictError
from ..models import (
    Inventory,
    Job,
    JobHandle,
    JobResult,
    JobStatus,
    Project,
    ProvisioningReport,
    RetryPolicy,
)
from ..provisioning import ProvisioningOutcome, _default_transport
from ..scheduling import HostRuntime, Lease, LeaseService
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
        raise NotImplementedError  # full body in Task 4

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
