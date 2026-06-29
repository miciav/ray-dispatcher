"""Local result tree: layout, output collection, atomic publish, manifests (spec §9.1).

All paths here are local to the dispatcher host. The Phase 5b attempt driver and
the Phase 6 backend call collect_outputs/publish_job_outputs and the manifest
writers; nothing in this module touches Ray.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from enum import Enum
from pathlib import Path

from .models import AttemptResult, JobResult


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


def _enc(o: object) -> object:
    """json default: enums serialize as their value; nothing else is allowed."""
    if isinstance(o, Enum):
        return o.value
    raise TypeError(f"not JSON serializable: {type(o).__name__}")


def write_attempt_json(
    path: Path, attempt: AttemptResult, *, missing_optional: tuple[str, ...] = ()
) -> None:
    """Write attempts/<n>/attempt.json (spec §9.1); record missing optional outputs (§7.8)."""
    doc = asdict(attempt)
    doc["missing_optional"] = list(missing_optional)
    path.write_text(json.dumps(doc, default=_enc, indent=2))


def write_result_json(path: Path, result: JobResult) -> None:
    """Write the job's result.json (spec §9.1), including its nested attempts."""
    path.write_text(json.dumps(asdict(result), default=_enc, indent=2))
