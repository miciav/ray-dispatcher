"""Atomic, heartbeat-backed remote session lock (spec §3.2.6, §6.3.1).

Drives a remote lock entirely through the Phase 2 Transport seam. Every command
is an ``sh -c`` script so the VM shell expands ``$HOME``; all data is shlex-quoted.
The lock is one file, created atomically via shell ``noclobber`` (``set -C``).
"""

from __future__ import annotations

import json
import shlex
import time
from collections.abc import Callable
from typing import Any

from .errors import HostInUseError
from .ssh import Transport

_LOCK_DIR = '"$HOME/.ray_dispatcher/locks"'
_LOCK_FILE = '"$HOME/.ray_dispatcher/locks/session.json"'
_LOCK_TMP = '"$HOME/.ray_dispatcher/locks/.session.json.tmp"'


def _sh(script: str) -> list[str]:
    return ["sh", "-c", script]


class SessionLock:
    def __init__(
        self,
        transport: Transport,
        session_id: str,
        *,
        ttl_s: float = 60.0,
        now: Callable[[], float] = time.time,
    ) -> None:
        self.transport = transport
        self.session_id = session_id
        self.ttl_s = ttl_s
        self.now = now

    def _payload(self) -> str:
        return json.dumps({"session_id": self.session_id, "heartbeat": self.now()})

    def _read_owner(self) -> dict[str, Any] | None:
        result = self.transport.run(_sh(f"cat {_LOCK_FILE} 2>/dev/null"))
        if result.returncode != 0 or not result.stdout.strip():
            return None
        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError:
            return None
        return data if isinstance(data, dict) else None

    def _write_owner(self) -> None:
        owner = shlex.quote(self._payload())
        self.transport.run(
            _sh(f"printf %s {owner} > {_LOCK_TMP} && mv -f {_LOCK_TMP} {_LOCK_FILE}")
        )

    def acquire(self) -> None:
        self.transport.run(_sh(f"mkdir -p {_LOCK_DIR}"))
        owner = shlex.quote(self._payload())
        created = self.transport.run(_sh(f"set -C; printf %s {owner} > {_LOCK_FILE}"))
        if created.returncode == 0:
            return  # created atomically -> we own it
        existing = self._read_owner()
        if existing is None or existing.get("session_id") == self.session_id:
            self._write_owner()  # ours, or unreadable/corrupt -> take it
            return
        if self.now() - float(existing.get("heartbeat", 0)) > self.ttl_s:
            self._write_owner()  # stale: heartbeat expired -> take over
            return
        raise HostInUseError(
            f"session lock held by {existing.get('session_id')!r}"
        )
