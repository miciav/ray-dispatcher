"""Per-host provisioning (spec §6.3) and inventory orchestration.

Drives each host through the §6.3 steps over the Phase 2 Transport seam. The
remote $HOME is resolved once per host (one `printf %s "$HOME"` probe), after
which every remote path is absolute — so both `run` (via shlex.join) and rsync
`push` (which uses --protect-args and would NOT expand `~`) are unambiguous.
All interpolated data is shlex-quoted; no user string is ever shelled (§7).
"""

from __future__ import annotations

import shlex

from .models import Project, RemoteHost
from .ssh import CommandResult, Transport


class _StepError(Exception):
    """A provisioning step failed on one host. Caught by the host driver and
    turned into a failed HostProvisioningResult; never escapes provisioning.py."""


class RemoteLayout:
    """Absolute remote paths under <home>/.ray_dispatcher (spec §6.1)."""

    def __init__(self, home: str, project_id: str) -> None:
        home = home.rstrip("/")
        self.root = f"{home}/.ray_dispatcher"
        self.project = f"{self.root}/projects/{project_id}"
        self.source = f"{self.project}/source"
        self.source_manifest = f"{self.project}/source-manifest.json"
        self.secrets = f"{self.root}/secrets/{project_id}"
        self.uv_root = f"{self.root}/uv"

    def env_dir(self, environment_digest: str) -> str:
        return f"{self.project}/envs/{environment_digest}"

    def env_venv(self, environment_digest: str) -> str:
        return f"{self.env_dir(environment_digest)}/.venv"

    def env_manifest(self, environment_digest: str) -> str:
        return f"{self.env_dir(environment_digest)}/environment-manifest.json"

    def runner_dir(self, runner_digest: str) -> str:
        return f"{self.root}/bin/{runner_digest}"

    def runner(self, runner_digest: str) -> str:
        return f"{self.runner_dir(runner_digest)}/remote_runner.py"

    def uv_bin(self, uv_version: str) -> str:
        return f"{self.uv_root}/{uv_version}/uv"


class HostProvisioner:
    """Provisions one host. `layout` is None until the driver resolves $HOME."""

    def __init__(
        self,
        transport: Transport,
        project: Project,
        host: RemoteHost,
        *,
        runner_path: str,
        session_id: str,
        force: bool = False,
        min_disk_mb: int = 500,
        heartbeat_interval_s: float = 20.0,
    ) -> None:
        self.t = transport
        self.project = project
        self.host = host
        self.runner_path = runner_path
        self.session_id = session_id
        self.force = force
        self.min_disk_mb = min_disk_mb
        self.heartbeat_interval_s = heartbeat_interval_s
        self.layout: RemoteLayout | None = None

    # --- helpers -------------------------------------------------------------

    def _checked(
        self, argv: list[str], what: str, *, timeout_s: float | None = None
    ) -> CommandResult:
        r = self.t.run(argv, timeout_s=timeout_s)
        if r.returncode != 0:
            detail = (r.stderr or r.stdout).strip()
            raise _StepError(f"{what} failed on {self.host.host} (rc={r.returncode}): {detail}")
        return r

    def _write_remote_file(self, path: str, content: str, *, mode: int | None = None) -> None:
        tmp = path + ".tmp"
        qtmp, qpath = shlex.quote(tmp), shlex.quote(path)
        chmod = f" && chmod {mode:o} {qtmp}" if mode is not None else ""
        self._checked(
            ["sh", "-c", f"printf %s {shlex.quote(content)} > {qtmp}{chmod} && mv -f {qtmp} {qpath}"],
            f"write {path}",
        )

    def _resolve_layout(self) -> RemoteLayout:
        home = self._checked(["sh", "-c", 'printf %s "$HOME"'], "resolve $HOME").stdout.strip()
        if not home:
            raise _StepError(f"could not resolve remote $HOME on {self.host.host}")
        return RemoteLayout(home, self.project.project_id)

    @property
    def _lo(self) -> RemoteLayout:
        if self.layout is None:  # pragma: no cover - guarded by driver ordering
            raise _StepError("layout not resolved")
        return self.layout
