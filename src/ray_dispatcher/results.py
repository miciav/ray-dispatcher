"""Local result tree: layout, output collection, atomic publish, manifests (spec §9.1).

All paths here are local to the dispatcher host. The Phase 5b attempt driver and
the Phase 6 backend call collect_outputs/publish_job_outputs and the manifest
writers; nothing in this module touches Ray.
"""

from __future__ import annotations

from pathlib import Path


class JobLayout:
    """Local result paths for one job: <results_dir>/<batch_id>/<job_id>/ (spec §9.1)."""

    def __init__(self, results_dir: str, batch_id: str, job_id: str) -> None:
        self.job_dir = Path(results_dir) / batch_id / job_id

    @property
    def attempts_dir(self) -> Path:
        return self.job_dir / "attempts"

    @property
    def outputs_dir(self) -> Path:
        return self.job_dir / "outputs"

    @property
    def result_json(self) -> Path:
        return self.job_dir / "result.json"

    def attempt_dir(self, n: int) -> Path:
        return self.attempts_dir / str(n)

    def stdout_log(self, n: int) -> Path:
        return self.attempt_dir(n) / "stdout.log"

    def stderr_log(self, n: int) -> Path:
        return self.attempt_dir(n) / "stderr.log"

    def attempt_json(self, n: int) -> Path:
        return self.attempt_dir(n) / "attempt.json"


def create_attempt_dir(layout: JobLayout, n: int) -> Path:
    """Create attempts/<n>; reusing an attempt number is a bug (spec §9.1)."""
    d = layout.attempt_dir(n)
    d.mkdir(parents=True)  # exist_ok=False -> FileExistsError if the attempt dir exists
    return d
