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
        affected = {ls.host for ls in self._leases.values() if ls.expiry_s < now}
        for host in affected:
            self.quarantine(host)
        return sorted(affected)
