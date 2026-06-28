#!/usr/bin/env python3
"""Standalone remote subprocess supervisor — runs ON the VM (spec §7).

Invoked as: ``python remote_runner.py <manifest.json>``. Stdlib only; it must
NOT import from ray_dispatcher, because it runs in the project venv where the
package is absent. The job argv runs via Popen (no shell).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from typing import IO, Any


def build_env(manifest: dict[str, Any]) -> dict[str, str]:
    env = os.environ.copy()
    env.update(manifest.get("env", {}))
    env.update(manifest.get("secret_env", {}))
    # venv guarantees win over any job-provided env (spec §7)
    env["PATH"] = manifest["venv_bin"] + os.pathsep + env.get("PATH", "")
    env["VIRTUAL_ENV"] = manifest["virtual_env"]
    return env


def _tee(stream: IO[bytes], raw_path: str, forward: IO[bytes]) -> None:
    """Write child output raw to ``raw_path`` and forward the same bytes live.
    Forwarding is best-effort: if the forward stream breaks (e.g. the host SSH
    channel drops), keep draining the pipe to the file so the child never blocks
    and the exit status is still recorded (spec §7)."""
    forwarding = True
    with open(raw_path, "wb") as raw:
        for chunk in iter(lambda: stream.read(4096), b""):
            raw.write(chunk)
            raw.flush()
            if forwarding:
                try:
                    forward.write(chunk)
                    forward.flush()
                except OSError:
                    forwarding = False


def run(manifest: dict[str, Any]) -> int:
    env = build_env(manifest)
    started = time.time()
    started_mono = time.monotonic()
    proc = subprocess.Popen(
        manifest["argv"],
        cwd=manifest["cwd"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    assert proc.stdout is not None and proc.stderr is not None
    with open(manifest["pid_path"], "w") as fh:
        json.dump({"pid": proc.pid, "pgid": os.getpgid(proc.pid)}, fh)
    threads = [
        threading.Thread(
            target=_tee, args=(proc.stdout, manifest["stdout_path"], sys.stdout.buffer)
        ),
        threading.Thread(
            target=_tee, args=(proc.stderr, manifest["stderr_path"], sys.stderr.buffer)
        ),
    ]
    for t in threads:
        t.start()
    proc.wait()
    for t in threads:
        t.join()
    ended = time.time()
    with open(manifest["result_path"], "w") as fh:
        json.dump(
            {
                "returncode": proc.returncode,
                "started_at": started,
                "ended_at": ended,
                "duration_s": time.monotonic() - started_mono,
            },
            fh,
        )
    return proc.returncode


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: remote_runner.py <manifest.json>", file=sys.stderr)
        return 2
    with open(argv[1]) as fh:
        manifest = json.load(fh)
    run(manifest)
    return 0  # the runner managed the child; the child's rc is in result.json


if __name__ == "__main__":
    sys.exit(main(sys.argv))
