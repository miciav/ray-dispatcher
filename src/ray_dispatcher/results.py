"""Local result tree: layout, output collection, atomic publish, manifests (spec §9.1).

All paths here are local to the dispatcher host. The Phase 5b attempt driver and
the Phase 6 backend call collect_outputs/publish_job_outputs and the manifest
writers; nothing in this module touches Ray.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path

from .models import AttemptResult, JobResult, OutputSpec
from .paths import ensure_within
from .ssh import Transport


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


@dataclass(frozen=True)
class CollectionResult:
    present: tuple[str, ...]
    missing_required: tuple[str, ...]
    missing_optional: tuple[str, ...]


def collect_outputs(
    transport: Transport,
    remote_run_dir: str,
    outputs: tuple[OutputSpec, ...],
    staging_dir: Path,
) -> CollectionResult:
    """Best-effort pull of declared outputs into attempt staging (spec §7.8).

    Each output's local destination is contained beneath staging_dir. After the
    pull, an output absent locally is classified: a missing required output
    drives OUTPUT_MISSING in the caller; a missing optional one is recorded.
    """
    present: list[str] = []
    missing_required: list[str] = []
    missing_optional: list[str] = []
    for spec in outputs:
        rel = spec.destination or spec.source
        dest = ensure_within(staging_dir, rel, field="output destination")
        dest.parent.mkdir(parents=True, exist_ok=True)
        transport.pull(f"{remote_run_dir}/{spec.source}", str(dest))
        if dest.exists():
            present.append(rel)
        elif spec.required:
            missing_required.append(rel)
        else:
            missing_optional.append(rel)
    return CollectionResult(tuple(present), tuple(missing_required), tuple(missing_optional))
