#!/usr/bin/env python3
"""Toy batch example: estimate pi with different configs on 2 Multipass VMs.

Usage (from repo root):
    uv run python examples/toy_batch/run_batch.py
"""

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from multipass import MultipassClient, MultipassVM, find_ssh_public_key

from ray_dispatcher import (
    Dispatcher,
    InputSpec,
    Inventory,
    Job,
    JobStatus,
    OutputSpec,
    Project,
    RemoteHost,
)

HERE = Path(__file__).parent
EXPERIMENT_DIR = HERE / "experiment"
CONFIGS_DIR = HERE / "configs"

N_VMS = 2
VM_PREFIX = "rd-toy"
SLOTS_PER_VM = 2  # 2 VMs × 2 slots = 4 concurrent jobs


def _cloud_init() -> dict:
    pub_key = find_ssh_public_key()
    if pub_key is None:
        raise RuntimeError("No SSH public key found in ~/.ssh/ — passwordless SSH required")
    return {
        "packages": ["rsync"],
        "ssh_authorized_keys": [pub_key],
        "package_update": True,
    }


def _scan_host_key(ip: str, *, retries: int = 10, interval: float = 3.0) -> str:
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


def main() -> None:
    if not (EXPERIMENT_DIR / "uv.lock").exists():
        print("Generating uv.lock for experiment project...")
        subprocess.run(["uv", "lock", "--project", str(EXPERIMENT_DIR)], check=True)

    client = MultipassClient()
    launched: list[tuple[MultipassVM, str]] = []

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        known_hosts = tmp_path / "known_hosts"
        results_dir = str(tmp_path / "results")

        try:
            print(f"Launching {N_VMS} VMs...")
            for i in range(N_VMS):
                name = f"{VM_PREFIX}-{i}"
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
                print(f"  {name}: ready at {ip}")

            print("Scanning host keys...")
            for _, ip in launched:
                with known_hosts.open("a") as f:
                    f.write(_scan_host_key(ip))

            inventory = Inventory(
                tuple(
                    RemoteHost(
                        host=ip,
                        user="ubuntu",
                        slots=SLOTS_PER_VM,
                        known_hosts_file=str(known_hosts),
                    )
                    for _, ip in launched
                )
            )

            project = Project(
                path=str(EXPERIMENT_DIR),
                project_id="rd-toy-batch",
                python="3.10.0",
                uv_version="0.11.25",
            )

            config_files = sorted(CONFIGS_DIR.glob("*.json"))
            jobs = [
                Job(
                    id=cfg.stem,
                    command=("python", "experiment.py", "cfg.json"),
                    inputs=(InputSpec(str(cfg), destination="cfg.json"),),
                    outputs=(OutputSpec("result.json"),),
                )
                for cfg in config_files
            ]

            print(f"\nDispatching {len(jobs)} jobs across {N_VMS} VMs "
                  f"({SLOTS_PER_VM} slots each)...")
            t0 = time.monotonic()

            with Dispatcher(inventory, project, results_dir=results_dir) as d:
                d.setup()
                results = d.run(jobs)

            elapsed = time.monotonic() - t0

            print(f"\nAll done in {elapsed:.1f}s\n")
            print(f"{'Job':<10} {'Status':<12} {'Result'}")
            print("-" * 60)
            for r in results:
                if r.status == JobStatus.SUCCEEDED and r.output_dir:
                    result_file = Path(r.output_dir) / "result.json"
                    if result_file.exists():
                        data = json.loads(result_file.read_text())
                        detail = f"pi ≈ {data['pi_estimate']:.6f}  ({data['n_samples']:,} samples)"
                    else:
                        detail = "(output not collected)"
                else:
                    detail = r.error or str(r.status)
                print(f"{r.id:<10} {r.status.value:<12} {detail}")

        finally:
            if launched:
                print("\nDestroying VMs...")
            for vm, _ in launched:
                try:
                    vm.delete(purge=True)
                    print(f"  {vm.name}: deleted")
                except Exception as exc:  # noqa: BLE001
                    print(f"  {vm.name}: delete failed: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
