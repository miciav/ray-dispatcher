"""Scheduling state machine: per-host slot leases with quarantine (spec §7.1, §8.2).

`LeasePool` is a pure, single-threaded, clock-injected state machine — no Ray,
no SSH. Phase 4b wraps it in the async HostLease Ray actor and adds the SSH
reconciliation probe.
"""

from __future__ import annotations

import secrets
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace

from .errors import ModelValidationError


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
