"""Host-side SSH/rsync transport and shared SSH options (spec §3.1, §4.1, §5)."""

from __future__ import annotations

import os
import shlex
import subprocess
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import fabric
import paramiko

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
    def from_host(cls, host: RemoteHost) -> SshConfig:
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
        "-o", "GlobalKnownHostsFile=/dev/null",
        "-o", "BatchMode=yes",
    ]
    if cfg.identity_file:
        parts += ["-i", cfg.identity_file]
    return shlex.join(parts)


def build_rsync_argv(
    cfg: SshConfig, src: str, dst: str, *, delete: bool, excludes: Sequence[str]
) -> list[str]:
    # --protect-args: stops the remote shell from mangling paths with spaces or special chars
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

    def run(self, argv: Sequence[str], *, timeout_s: float | None = None) -> CommandResult:
        if self._conn is None:
            self._conn = build_connection(self.cfg)
        command = shlex.join(argv)  # argv is library-controlled; never a user job string
        start = time.monotonic()
        try:
            result = self._conn.run(
                command, hide=True, warn=True, timeout=timeout_s, in_stream=False
            )
        except Exception as exc:  # noqa: BLE001 — uniform seam failure
            raise TransportError(f"ssh run failed: {exc}") from exc
        return CommandResult(
            returncode=result.exited,
            stdout=result.stdout,
            stderr=result.stderr,
            duration_s=time.monotonic() - start,
        )

    def _rsync(self, argv: list[str]) -> None:
        try:
            subprocess.run(argv, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as exc:
            raise TransportError(f"rsync failed ({exc.returncode}): {exc.stderr}") from exc


def terminate_process_group(
    transport: Transport,
    pgid: int,
    *,
    grace_s: float = 10.0,
    poll_s: float = 0.5,
    now: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    """SIGTERM the remote process group, wait up to grace_s, then SIGKILL.
    Returns True once a probe confirms the group is gone (spec §8.1)."""

    def gone() -> bool:
        return transport.run(["kill", "-0", f"-{pgid}"]).returncode != 0

    transport.run(["kill", "-TERM", f"-{pgid}"])
    deadline = now() + grace_s
    while now() < deadline:
        if gone():
            return True
        sleep(poll_s)
    transport.run(["kill", "-KILL", f"-{pgid}"])
    sleep(poll_s)
    return gone()


def write_remote_file(
    transport: Transport, path: str, content: str, *, mode: int | None = None
) -> None:
    """Atomically write ``content`` to absolute remote ``path`` (spec §7 no-shell).

    Data reaches the shell only as a single shlex-quoted ``printf %s`` argument;
    the temp file is renamed into place so a partial write is never observed.
    """
    qtmp = shlex.quote(f"{path}.tmp")
    chmod = f" && chmod {mode:o} {qtmp}" if mode is not None else ""
    cmd = f"printf %s {shlex.quote(content)} > {qtmp}{chmod} && mv -f {qtmp} {shlex.quote(path)}"
    result = transport.run(["sh", "-c", cmd])
    if result.returncode != 0:
        raise TransportError(f"failed to write {path}: {result.stderr}")


def build_connection(cfg: SshConfig) -> fabric.Connection:
    """A Fabric connection that enforces host-key checking against cfg.known_hosts_file."""
    connect_kwargs: dict[str, object] = {}
    if cfg.identity_file:
        connect_kwargs["key_filename"] = cfg.identity_file
    conn = fabric.Connection(
        host=cfg.host, user=cfg.user, port=cfg.port, connect_kwargs=connect_kwargs
    )
    client = conn.client  # lazily-created paramiko.SSHClient
    client.load_host_keys(cfg.known_hosts_file)
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    return conn
