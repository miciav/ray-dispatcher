"""Validated public value objects (spec §4)."""

from __future__ import annotations

import re
from dataclasses import dataclass

import yaml

from .errors import ModelValidationError

_POSIX_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_posix_env_name(name: str) -> bool:
    return bool(_POSIX_ENV_NAME_RE.match(name))


@dataclass(frozen=True)
class RemoteHost:
    host: str
    user: str
    slots: int = 1
    port: int = 22
    identity_file: str | None = None
    known_hosts_file: str = "~/.ssh/known_hosts"

    def __post_init__(self) -> None:
        if not isinstance(self.host, str) or not self.host:
            raise ModelValidationError("host must be a non-empty string")
        if not isinstance(self.user, str) or not self.user:
            raise ModelValidationError("user must be a non-empty string")
        if self.slots < 1:
            raise ModelValidationError(f"slots must be >= 1, got {self.slots}")
        if not (1 <= self.port <= 65535):
            raise ModelValidationError(f"port must be in 1..65535, got {self.port}")


@dataclass(frozen=True)
class Inventory:
    hosts: tuple[RemoteHost, ...]

    def __post_init__(self) -> None:
        if not self.hosts:
            raise ModelValidationError("inventory must contain at least one host")
        seen: set[tuple[str, int, str]] = set()
        for h in self.hosts:
            key = (h.host, h.port, h.user)
            if key in seen:
                raise ModelValidationError(f"duplicate host entry: {key}")
            seen.add(key)

    @classmethod
    def from_yaml(cls, path: str) -> "Inventory":
        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        if not isinstance(data, dict) or "hosts" not in data:
            raise ModelValidationError("inventory YAML must have a top-level 'hosts' list")
        raw_hosts = data["hosts"]
        if not isinstance(raw_hosts, list):
            raise ModelValidationError("'hosts' must be a list")
        hosts = tuple(RemoteHost(**entry) for entry in raw_hosts)
        return cls(hosts)
