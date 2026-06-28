"""Validated public value objects (spec §4)."""

from __future__ import annotations

import re
from dataclasses import dataclass

import yaml

from .errors import ModelValidationError

_POSIX_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_EXACT_VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")
_PROJECT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


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


@dataclass(frozen=True)
class SecretFile:
    source: str
    remote_name: str
    env_var: str | None = None
    mode: int = 0o600

    def __post_init__(self) -> None:
        if not self.source:
            raise ModelValidationError("secret source must be non-empty")
        if not self.remote_name or "/" in self.remote_name or self.remote_name in (".", ".."):
            raise ModelValidationError(
                f"secret remote_name must be a bare filename, got {self.remote_name!r}"
            )
        if self.env_var is not None and not _is_posix_env_name(self.env_var):
            raise ModelValidationError(f"invalid secret env_var: {self.env_var!r}")


@dataclass(frozen=True)
class Project:
    path: str
    project_id: str
    python: str
    uv_version: str
    secrets: tuple[SecretFile, ...] = ()
    exclude: tuple[str, ...] = (".venv/", ".git/", "solutions/")
    dependency_groups: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.path:
            raise ModelValidationError("project path must be non-empty")
        if not _PROJECT_ID_RE.match(self.project_id):
            raise ModelValidationError(f"invalid project_id: {self.project_id!r}")
        if not _EXACT_VERSION_RE.match(self.python):
            raise ModelValidationError(f"python must be exact X.Y.Z, got {self.python!r}")
        if not _EXACT_VERSION_RE.match(self.uv_version):
            raise ModelValidationError(
                f"uv_version must be exact X.Y.Z, got {self.uv_version!r}"
            )
        names = [s.remote_name for s in self.secrets]
        if len(names) != len(set(names)):
            raise ModelValidationError("duplicate secret remote_name in project")
