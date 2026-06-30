"""Multipass e2e smoke tests — full stack, real VMs."""

from pathlib import Path

import pytest

from ray_dispatcher import Dispatcher, InputSpec, Job, JobStatus, OutputSpec


@pytest.mark.e2e
def test_dispatcher_run_succeeds_on_real_vms(tmp_path, inventory, synth_project):
    """Provision + run 4 jobs on real VMs; verify all SUCCEEDED with result.json on disk."""
    project, proj_dir = synth_project
    results_dir = str(tmp_path / "results")

    jobs = [Job(id=f"smoke-{i}", command=("python", "run.py")) for i in range(4)]

    with Dispatcher(inventory, project, results_dir=results_dir) as d:
        d.setup()
        results = d.run(jobs)

    assert len(results) == 4
    assert [r.id for r in results] == [j.id for j in jobs]  # input order preserved
    for r in results:
        assert r.status == JobStatus.SUCCEEDED, f"job {r.id} failed: {r.error}"
        assert r.returncode == 0
        # result.json written to disk
        result_json = Path(results_dir) / results[0].batch_id / r.id / "result.json"
        assert result_json.exists(), f"result.json missing for {r.id}"


@pytest.mark.e2e
def test_dispatcher_setup_is_idempotent(tmp_path, inventory, synth_project):
    """Second Dispatcher.setup() is a no-op (cached provisioning report)."""
    project, _ = synth_project
    results_dir = str(tmp_path / "results")

    with Dispatcher(inventory, project, results_dir=results_dir) as d:
        r1 = d.setup()
        r2 = d.setup()
        assert r1 is r2
        # All hosts succeeded
        assert all(h.succeeded for h in r1.hosts)


@pytest.mark.e2e
def test_failed_job_returns_failed_status(tmp_path, inventory, synth_project):
    """A job that exits with returncode=1 returns JobStatus.FAILED."""
    project, proj_dir = synth_project

    # Write a config file that tells run.py to fail.
    cfg_path = tmp_path / "fail_cfg.json"
    cfg_path.write_text('{"fail": true}')

    results_dir = str(tmp_path / "results")
    job = Job(id="will-fail", command=("python", "run.py", str(cfg_path)))

    with Dispatcher(inventory, project, results_dir=results_dir) as d:
        d.setup()
        results = d.run([job])

    assert results[0].status == JobStatus.FAILED
    assert results[0].returncode == 1


@pytest.mark.e2e
def test_output_file_collected(tmp_path, inventory, synth_project):
    """A job that writes an output file has it collected to the local results dir."""
    project, proj_dir = synth_project
    results_dir = str(tmp_path / "results")

    # The job writes "out.txt" in the working dir; we declare it as an OutputSpec.
    _job = Job(  # ponytail: intentional duplicate — shows shape without cfg vs with cfg
        id="with-output",
        command=("python", "run.py"),
        outputs=(OutputSpec("out.txt", required=True),),
    )

    # Write config so run.py creates out.txt.
    cfg_path = tmp_path / "out_cfg.json"
    cfg_path.write_text('{"output": "out.txt"}')

    job_with_cfg = Job(
        id="with-output",
        command=("python", "run.py", "out_cfg.json"),
        inputs=(InputSpec(str(cfg_path), destination="out_cfg.json"),),
        outputs=(OutputSpec("out.txt", required=True),),
    )

    with Dispatcher(inventory, project, results_dir=results_dir) as d:
        d.setup()
        results = d.run([job_with_cfg])

    r = results[0]
    assert r.status == JobStatus.SUCCEEDED
    # The output should be collected under the job's output_dir.
    if r.output_dir is not None:
        collected = Path(r.output_dir) / "out.txt"
        assert collected.exists(), f"out.txt not collected to {r.output_dir}"
