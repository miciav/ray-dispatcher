"""Host-side SSH/rsync transport and shared SSH options (spec §3.1, §4.1, §5)."""

from __future__ import annotations

import os
from dataclasses import dataclass

from .errors import ModelValidationError
from .models import RemoteHost


def _resolve_existing(path: str, label: str) -> str:
    resolved = os.path.abspath(os.path.expanduser(path))
    if not os.path.isfile(resolved):
        raise ModelValidationError(f"{label} not found: {path}")
    return resolved


@dataclass(frozen=True)
class SshConfig:
    host: str
    user: str
    port: int
    identity_file: str | None
    known_hosts_file: str

    @classmethod
    def from_host(cls, host: RemoteHost) -> "SshConfig":
        known_hosts = _resolve_existing(host.known_hosts_file, "known_hosts file")
        identity = (
            _resolve_existing(host.identity_file, "identity file")
            if host.identity_file is not None
            else None
        )
        return cls(
            host=host.host,
            user=host.user,
            port=host.port,
            identity_file=identity,
            known_hosts_file=known_hosts,
        )
