"""Per-host provisioning (spec §6.3) and inventory orchestration.

Drives each host through the §6.3 steps over the Phase 2 Transport seam. The
remote $HOME is resolved once per host (one `printf %s "$HOME"` probe), after
which every remote path is absolute — so both `run` (via shlex.join) and rsync
`push` (which uses --protect-args and would NOT expand `~`) are unambiguous.
All interpolated data is shlex-quoted; no user string is ever shelled (§7).
"""

from __future__ import annotations

import json
import shlex

from .digests import environment_digest, runner_digest, source_digest
from .models import Project, RemoteHost
from .ssh import CommandResult, Transport

# Base sync flags shared by environment_digest and the uv sync invocation (§6.3.6).
SYNC_FLAGS = (
    "--locked",
    "--no-install-project",
    "--no-install-workspace",
    "--no-default-groups",
)


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
        cmd = f"printf %s {shlex.quote(content)} > {qtmp}{chmod} && mv -f {qtmp} {qpath}"
        self._checked(
            ["sh", "-c", cmd],
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

    def _preflight(self) -> None:
        for tool in ("python3", "rsync"):
            if self.t.run(["command", "-v", tool]).returncode != 0:
                raise _StepError(f"required tool {tool!r} missing on {self.host.host}")
        root = shlex.quote(self._lo.root)
        # df -Pk gives POSIX one-line-per-fs output; available 1024-blocks is column 4.
        r = self._checked(
            ["sh", "-c", f"mkdir -p {root} && df -Pk {root} | tail -1"], "disk check"
        )
        try:
            avail_kb = int(r.stdout.split()[3])
        except (IndexError, ValueError) as exc:
            msg = f"could not parse disk free on {self.host.host}: {r.stdout!r}"
            raise _StepError(msg) from exc
        if avail_kb < self.min_disk_mb * 1024:
            raise _StepError(
                f"insufficient disk on {self.host.host}: "
                f"{avail_kb // 1024} MB < {self.min_disk_mb} MB required"
            )

    def _install_uv(self) -> str:
        ver = self.project.uv_version
        uv = self._lo.uv_bin(ver)
        if not self.force and ver in self.t.run([uv, "--version"]).stdout.split():
            return uv
        install_dir = f"{self._lo.uv_root}/{ver}"
        # ponytail: official version-pinned installer; exact on-disk layout
        #           (UV_INSTALL_DIR -> <dir>/uv) is reconfirmed by the Phase 7 e2e.
        # ver is safe in the URL: Project validates uv_version as r"\d+\.\d+\.\d+"
        # (fullmatch) -> only digits and dots, no shell metacharacters (§7).
        script = (
            f"set -e; mkdir -p {shlex.quote(install_dir)}; "
            f"curl -LsSf https://astral.sh/uv/{ver}/install.sh "
            f"| env UV_INSTALL_DIR={shlex.quote(install_dir)} INSTALLER_NO_MODIFY_PATH=1 sh"
        )
        self._checked(["sh", "-c", script], "uv install")
        reported = self._checked([uv, "--version"], "uv version check").stdout
        if ver not in reported.split():
            raise _StepError(
                f"installed uv on {self.host.host} reports {reported.strip()!r}, expected {ver}"
            )
        return uv

    def _install_python(self, uv: str) -> None:
        want = self.project.python
        self._checked([uv, "python", "install", want], "uv python install")
        interp = self._checked([uv, "python", "find", want], "uv python find").stdout.strip()
        if not interp:
            raise _StepError(f"uv could not locate Python {want} on {self.host.host}")
        got = self._checked(
            [interp, "-c", "import sys;print('.'.join(map(str,sys.version_info[:3])))"],
            "python version check",
        ).stdout.strip()
        if got != want:
            raise _StepError(
                f"interpreter on {self.host.host} is {got!r}, expected {want!r}"
            )

    def _sync_source(self) -> str:
        staging = f"{self._lo.source}.staging"
        self._checked(["sh", "-c", f"mkdir -p {shlex.quote(staging)}"], "source staging mkdir")
        # trailing slashes: copy the *contents* of the local tree into staging.
        self.t.push(
            self.project.path.rstrip("/") + "/",
            staging + "/",
            delete=True,
            excludes=self.project.exclude,
        )
        src = shlex.quote(self._lo.source)
        stg = shlex.quote(staging)
        old = shlex.quote(self._lo.source + ".old")
        # The `mv staging source` rename is atomic within one parent dir; the brief
        # window where source is absent is acceptable pre-runtime (no readers yet).
        self._checked(
            ["sh", "-c", f"rm -rf {old}; if [ -e {src} ]; then mv {src} {old}; fi; "
                         f"mv {stg} {src}; rm -rf {old}"],
            "source atomic replace",
        )
        digest = source_digest(self.project.path, self.project.exclude)
        manifest = json.dumps({"source_digest": digest, "project_id": self.project.project_id})
        self._write_remote_file(self._lo.source_manifest, manifest)
        return digest

    def _publish_env(self, uv: str) -> str:
        platform = self._checked(["uname", "-sm"], "platform probe").stdout.strip()
        digest = environment_digest(self.project, platform=platform, sync_flags=SYNC_FLAGS)
        env_dir = self._lo.env_dir(digest)
        venv = self._lo.env_venv(digest)
        manifest_path = self._lo.env_manifest(digest)
        valid = self.t.run(
            ["sh", "-c", f"test -f {shlex.quote(manifest_path)} && "
                         f"test -x {shlex.quote(venv)}/bin/python"]
        ).returncode == 0
        if valid and not self.force:
            return digest

        staging = f"{env_dir}.staging"
        staging_venv = f"{staging}/.venv"
        self._checked(
            ["sh", "-c", f"rm -rf {shlex.quote(staging)}; mkdir -p {shlex.quote(staging)}"],
            "env staging mkdir",
        )
        sync = [uv, "sync", "--project", self._lo.source, *SYNC_FLAGS,
                "--python", self.project.python]
        for group in self.project.dependency_groups:
            sync += ["--group", group]
        self._checked(
            ["sh", "-c",
             f"UV_PROJECT_ENVIRONMENT={shlex.quote(staging_venv)} {shlex.join(sync)}"],
            "uv sync",
        )
        # ponytail: venv relocatability after the atomic move is reconfirmed by the
        #           Phase 7 e2e; bin/python is a symlink to the uv interpreter and
        #           survives a move, console-script shebangs would not (jobs use python).
        self._checked(
            ["sh", "-c", f"{shlex.quote(staging_venv)}/bin/python -c 'import sys'"],
            "venv smoke check",
        )
        manifest = json.dumps({
            "environment_digest": digest,
            "python": self.project.python,
            "uv_version": self.project.uv_version,
            "platform": platform,
            "dependency_groups": list(self.project.dependency_groups),
            "sync_flags": list(SYNC_FLAGS),
        })
        self._write_remote_file(f"{staging}/environment-manifest.json", manifest)
        qenv, qstg = shlex.quote(env_dir), shlex.quote(staging)
        qold = shlex.quote(env_dir + ".old")
        self._checked(
            ["sh", "-c", f"mkdir -p {shlex.quote(self._lo.project)}/envs; rm -rf {qold}; "
                         f"if [ -e {qenv} ]; then mv {qenv} {qold}; fi; "
                         f"mv {qstg} {qenv}; rm -rf {qold}"],
            "env atomic publish",
        )
        return digest

    def _install_runner(self) -> str:
        digest = runner_digest(self.runner_path)
        remote = self._lo.runner(digest)
        present = self.t.run(["test", "-f", remote]).returncode == 0
        if present and not self.force:
            return digest
        self._checked(
            ["sh", "-c", f"mkdir -p {shlex.quote(self._lo.runner_dir(digest))}"],
            "runner dir mkdir",
        )
        self.t.push(self.runner_path, remote)
        return digest
