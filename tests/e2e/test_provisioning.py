"""Multipass e2e provisioning scenario tests — §11 remaining scenarios."""

import pytest

from ray_dispatcher import Dispatcher, FailureKind, Job, JobStatus, OutputSpec


@pytest.mark.e2e
def test_missing_required_output_fails_job(tmp_path, inventory, synth_project):
    """Job declares required output but never creates it — must fail with OUTPUT_MISSING (§11)."""
    project, _ = synth_project
    results_dir = str(tmp_path / "results")

    # run.py with no args exits 0 but creates no files
    job = Job(
        id="no-output",
        command=("python", "run.py"),
        outputs=(OutputSpec("missing.txt", required=True),),
    )

    with Dispatcher(inventory, project, results_dir=results_dir) as d:
        d.setup()
        results = d.run([job])

    r = results[0]
    assert r.status == JobStatus.FAILED, f"expected FAILED, got {r.status}: {r.error}"
    assert len(r.attempts) >= 1
    assert r.attempts[-1].failure_kind == FailureKind.OUTPUT_MISSING, (
        f"expected OUTPUT_MISSING, got {r.attempts[-1].failure_kind}"
    )
