"""Host-side SSH/rsync transport and shared SSH options (spec §3.1, §4.1, §5)."""

from __future__ import annotations

import os
import shlex
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from .errors import DispatcherError, ModelValidationError
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


class TransportError(DispatcherError):
    """A transport (ssh/rsync) operation failed. Phase 5 maps this to FailureKind.SSH."""


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str
    duration_s: float

    @property
    def ok(self) -> bool:
        return self.returncode == 0


class Transport(Protocol):
    def run(self, argv: Sequence[str], *, timeout_s: float | None = None) -> CommandResult: ...

    def push(
        self, local: str, remote: str, *, delete: bool = False, excludes: Sequence[str] = ()
    ) -> None: ...

    def pull(
        self, remote: str, local: str, *, delete: bool = False, excludes: Sequence[str] = ()
    ) -> None: ...


class FakeTransport:
    """In-memory Transport for unit tests. Records calls; programmable run results."""

    def __init__(self, run_results: Callable[[list[str]], CommandResult] | None = None) -> None:
        self.calls: list[tuple[object, ...]] = []
        self._run_results = run_results

    def run(self, argv: Sequence[str], *, timeout_s: float | None = None) -> CommandResult:
        self.calls.append(("run", tuple(argv)))
        if self._run_results is not None:
            return self._run_results(list(argv))
        return CommandResult(0, "", "", 0.0)

    def push(
        self, local: str, remote: str, *, delete: bool = False, excludes: Sequence[str] = ()
    ) -> None:
        self.calls.append(("push", (local, remote, delete, tuple(excludes))))

    def pull(
        self, remote: str, local: str, *, delete: bool = False, excludes: Sequence[str] = ()
    ) -> None:
        self.calls.append(("pull", (remote, local, delete, tuple(excludes))))


def _ssh_e_option(cfg: SshConfig) -> str:
    """The rsync ``-e`` value: an ssh invocation carrying the same SSH settings
    as Fabric. shlex.join keeps paths with spaces safe."""
    parts = [
        "ssh",
        "-p", str(cfg.port),
        "-o", f"UserKnownHostsFile={cfg.known_hosts_file}",
        "-o", "StrictHostKeyChecking=yes",
    ]
    if cfg.identity_file:
        parts += ["-i", cfg.identity_file]
    return shlex.join(parts)


def build_rsync_argv(
    cfg: SshConfig, src: str, dst: str, *, delete: bool, excludes: Sequence[str]
) -> list[str]:
    argv = ["rsync", "-a", "--protect-args", "-e", _ssh_e_option(cfg)]
    if delete:
        argv.append("--delete")
    for ex in excludes:
        argv += ["--exclude", ex]
    argv += [src, dst]
    return argv


class SshTransport:
    """Fabric for run (Task 5), OpenSSH rsync for push/pull. The Fabric
    connection is created lazily so file transfer needs no live connection."""

    def __init__(self, cfg: SshConfig) -> None:
        self.cfg = cfg
        self._conn: Any = None  # lazily-built fabric.Connection (Task 5)

    def push(
        self, local: str, remote: str, *, delete: bool = False, excludes: Sequence[str] = ()
    ) -> None:
        dst = f"{self.cfg.user}@{self.cfg.host}:{remote}"
        self._rsync(build_rsync_argv(self.cfg, local, dst, delete=delete, excludes=excludes))

    def pull(
        self, remote: str, local: str, *, delete: bool = False, excludes: Sequence[str] = ()
    ) -> None:
        src = f"{self.cfg.user}@{self.cfg.host}:{remote}"
        self._rsync(build_rsync_argv(self.cfg, src, local, delete=delete, excludes=excludes))

    def _rsync(self, argv: list[str]) -> None:
        try:
            subprocess.run(argv, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as exc:
            raise TransportError(f"rsync failed ({exc.returncode}): {exc.stderr}") from exc
