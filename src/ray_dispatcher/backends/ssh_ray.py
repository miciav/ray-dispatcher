"""Exclusive local-Ray execution backend (spec §3.2, §3.3).

This is the only module that imports Ray. The HostLease actor wraps the Ray-free
LeaseService; SshRayBackend owns one local Ray runtime, started after provisioning
and shut down at teardown.
"""

from __future__ import annotations

from collections.abc import Iterable

import ray

from ..scheduling import Lease, LeaseService

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
