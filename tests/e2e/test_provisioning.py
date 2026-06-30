"""Multipass e2e provisioning scenario tests — §11 remaining scenarios."""

import shutil
import subprocess
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
    assert env_digests_1.keys() == env_digests_2.keys(), "host sets differ between runs"
    for host in env_digests_1:
        assert env_digests_1[host] is not None, (
            f"env_digest is None for {host} — provisioning failed"
        )
        assert env_digests_1[host] == env_digests_2[host], (
            f"env_digest changed on {host} — expected cache hit"
        )

    # Second setup skips uv sync — should be noticeably faster than first
    # (First: install uv + python + uv sync + runner. Second: just rsync source.)
    # Allow 60s to be generous; without sync, expect <30s.
    assert setup_elapsed < 60.0, f"Second setup took {setup_elapsed:.1f}s — expected cache hit"


@pytest.mark.e2e
def test_lockfile_change_rebuilds_env(tmp_path, inventory, synth_project):
    """Adding a dep + re-locking triggers uv sync; new dep importable in jobs (§11)."""
    _, proj_dir = synth_project

    mutable_dir = tmp_path / "project_lockchange"
    shutil.copytree(proj_dir, mutable_dir)

    project = Project(
        path=str(mutable_dir),
        project_id="rd-e2e-lockfile-change",
        python="3.10.0",
        uv_version="0.11.25",
    )

    results_dir_1 = str(tmp_path / "results_lc1")
    job_baseline = Job(id="baseline", command=("python", "run.py"))

    # First run — baseline env
    with Dispatcher(inventory, project, results_dir=results_dir_1) as d:
        report_1 = d.setup()
        r = d.run([job_baseline])
    assert r[0].status == JobStatus.SUCCEEDED

    # Add tomli dependency and regenerate lockfile
    pyproject = mutable_dir / "pyproject.toml"
    content = pyproject.read_text()
    new_content = content.replace(
        'version = "0.1.0"\n',
        'version = "0.1.0"\ndependencies = ["tomli>=2.0,<3"]\n',
    )
    pyproject.write_text(new_content)
    result = subprocess.run(
        ["uv", "lock", "--project", str(mutable_dir)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"uv lock failed: {result.stderr}"

    # Overwrite run.py to prove the new dep is installed in the rebuilt env
    (mutable_dir / "run.py").write_text(
        "import tomli; print('tomli imported ok')\n"
    )

    results_dir_2 = str(tmp_path / "results_lc2")
    job_with_dep = Job(id="with-dep", command=("python", "run.py"))

    with Dispatcher(inventory, project, results_dir=results_dir_2) as d:
        report_2 = d.setup()
        r2 = d.run([job_with_dep])

    assert r2[0].status == JobStatus.SUCCEEDED, f"job with tomli dep failed: {r2[0].error}"

    # Environment digest must differ (lockfile changed → new env)
    env_digests_1 = {h.host: h.environment_digest for h in report_1.hosts}
    env_digests_2 = {h.host: h.environment_digest for h in report_2.hosts}
    assert env_digests_1.keys() == env_digests_2.keys(), "host sets differ between runs"
    for host in env_digests_1:
        assert env_digests_1[host] is not None, (
            f"env_digest is None for {host} — first provisioning failed"
        )
        assert env_digests_2[host] is not None, (
            f"env_digest is None for {host} — second provisioning failed"
        )
        assert env_digests_1[host] != env_digests_2[host], (
            f"env_digest identical on {host} — expected env rebuild after lockfile change"
        )
