"""Session-scoped fixtures for Multipass e2e tests."""

import subprocess
import time
from pathlib import Path

import pytest
from multipass import MultipassClient, MultipassVM, find_ssh_public_key

from ray_dispatcher.models import Inventory, Project, RemoteHost

# Number of VMs to launch for e2e tests. 2 is enough to test concurrency.
_N_VMS = 2
_SLOTS_PER_VM = 2
_VM_PREFIX = "rd-test"


def _cloud_init() -> dict:
    pub_key = find_ssh_public_key()
    if pub_key is None:
        raise RuntimeError(
            "No SSH public key found in ~/.ssh/ — e2e tests require passwordless SSH"
        )
    return {
        "packages": ["rsync"],
        "ssh_authorized_keys": [pub_key],
        "package_update": True,
    }


def _scan_host_key(ip: str, *, retries: int = 10, interval: float = 3.0) -> str:
    """Run ssh-keyscan against ip, retrying until it succeeds."""
    for _ in range(retries):
        result = subprocess.run(
            ["ssh-keyscan", "-T", "5", ip],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout
        time.sleep(interval)
    raise RuntimeError(f"ssh-keyscan {ip} failed after {retries} retries")


@pytest.fixture(scope="session")
def vm_fleet(tmp_path_factory: pytest.TempPathFactory):  # noqa: ARG001
    """Launch _N_VMS Multipass VMs. Yields list of (vm, ip). Deletes all in finally."""
    client = MultipassClient()
    launched: list[tuple[MultipassVM, str]] = []
    try:
        for i in range(_N_VMS):
            name = f"{_VM_PREFIX}-{i}"
            vm = client.launch(
                name=name,
                image="22.04",
                cpus=1,
                memory="1G",
                disk="6G",
                cloud_init_config=_cloud_init(),
            )
            ip = vm.wait_ready(timeout=180, port=22)
            vm.exec(["cloud-init", "status", "--wait"])
            launched.append((vm, ip))
        yield launched
    finally:
        for vm, _ in launched:
            try:
                vm.delete(purge=True)
            except Exception:  # noqa: BLE001
                pass


@pytest.fixture(scope="session")
def known_hosts_path(tmp_path_factory: pytest.TempPathFactory, vm_fleet) -> Path:
    """Scan host keys for all VMs into a single known_hosts file."""
    kh = tmp_path_factory.mktemp("ssh") / "known_hosts"
    for _, ip in vm_fleet:
        with kh.open("a") as f:
            f.write(_scan_host_key(ip))
    return kh


@pytest.fixture(scope="session")
def inventory(vm_fleet, known_hosts_path) -> Inventory:
    """Inventory of all e2e VMs."""
    hosts = tuple(
        RemoteHost(
            host=ip,
            user="ubuntu",
            slots=_SLOTS_PER_VM,
            known_hosts_file=str(known_hosts_path),
        )
        for _, ip in vm_fleet
    )
    return Inventory(hosts)


@pytest.fixture(scope="session")
def synth_project(tmp_path_factory: pytest.TempPathFactory) -> tuple[Project, Path]:
    """Minimal synthetic project: pyproject.toml + run.py."""
    proj_dir = tmp_path_factory.mktemp("synth_project")

    (proj_dir / "pyproject.toml").write_text(
        "[project]\n"
        'name = "synth"\n'
        'version = "0.1.0"\n'
        "\n"
        "[tool.uv]\n"
        "package = false\n",
        encoding="utf-8",
    )

    (proj_dir / "run.py").write_text(
        "import sys, time, json, pathlib\n"
        "cfg = json.loads(pathlib.Path(sys.argv[1]).read_text()) if len(sys.argv) > 1 else {}\n"
        "time.sleep(cfg.get('sleep', 0))\n"
        "out = cfg.get('output', '')\n"
        "if out:\n"
        "    pathlib.Path(out).write_text('done')\n"
        "if cfg.get('fail', False):\n"
        "    sys.exit(1)\n"
        "print(cfg.get('msg', 'ok'))\n",
        encoding="utf-8",
    )

    project = Project(
        path=str(proj_dir),
        project_id="rd-e2e-synth",
        python="3.10.0",
        uv_version="0.11.25",
    )
    return project, proj_dir
