"""Multipass e2e provisioning scenario tests — §11 remaining scenarios."""

import shutil
import time

import pytest

from ray_dispatcher import Dispatcher, FailureKind, Job, JobStatus, OutputSpec, Project


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


@pytest.mark.e2e
def test_source_only_change_reuses_env(tmp_path, inventory, synth_project):
    """Changing only run.py rsyncs source but skips uv sync (env_digest unchanged) (§11)."""
    _, proj_dir = synth_project

    # Copy synth project to a mutable per-test dir
    mutable_dir = tmp_path / "project"
    shutil.copytree(proj_dir, mutable_dir)

    project_v1 = Project(
        path=str(mutable_dir),
        project_id="rd-e2e-source-change",
        python="3.10.0",
        uv_version="0.11.25",
    )

    results_dir_1 = str(tmp_path / "results1")
    job_v1 = Job(id="source-v1", command=("python", "run.py"))

    # First run
    report_1 = None
    with Dispatcher(inventory, project_v1, results_dir=results_dir_1) as d:
        report_1 = d.setup()
        results_1 = d.run([job_v1])

    assert results_1[0].status == JobStatus.SUCCEEDED

    # Mutate run.py (source change only — pyproject.toml and uv.lock untouched)
    (mutable_dir / "run.py").write_text(
        "import sys, json, pathlib\n"
        "cfg = json.loads(pathlib.Path(sys.argv[1]).read_text()) if len(sys.argv) > 1 else {}\n"
        "print('v2:' + cfg.get('msg', 'ok'))\n"
    )

    results_dir_2 = str(tmp_path / "results2")
    job_v2 = Job(id="source-v2", command=("python", "run.py"))

    # Second run — same project_id, same env
    report_2 = None
    with Dispatcher(inventory, project_v1, results_dir=results_dir_2) as d:
        t0 = time.monotonic()
        report_2 = d.setup()
        setup_elapsed = time.monotonic() - t0
        results_2 = d.run([job_v2])

    assert results_2[0].status == JobStatus.SUCCEEDED

    # Env digest must be the same (uv sync was skipped)
    env_digests_1 = {h.host: h.environment_digest for h in report_1.hosts}
    env_digests_2 = {h.host: h.environment_digest for h in report_2.hosts}
    for host in env_digests_1:
        assert env_digests_1[host] == env_digests_2[host], (
            f"env_digest changed on {host} — expected cache hit"
        )

    # Second setup skips uv sync — should be noticeably faster than first
    # (First: install uv + python + uv sync + runner. Second: just rsync source.)
    # Allow 60s to be generous; without sync, expect <30s.
    assert setup_elapsed < 60.0, f"Second setup took {setup_elapsed:.1f}s — expected cache hit"
