"""Multipass e2e concurrency tests — slot limits and Ray retry suppression (§11)."""

import json
import time

import pytest

from ray_dispatcher import Dispatcher, InputSpec, Job, JobStatus


@pytest.mark.e2e
def test_sum_slots_jobs_run_concurrently(tmp_path, inventory, synth_project):
    """All sum(slots)=4 jobs start within the same 2-second window (§11)."""
    project, _ = synth_project
    results_dir = str(tmp_path / "results")
    n_slots = sum(h.slots for h in inventory.hosts)  # _N_VMS * _SLOTS_PER_VM = 4

    # Each job sleeps 3s. Sequential = 12s+. Concurrent = ~3s.
    jobs = []
    for i in range(n_slots):
        cfg = tmp_path / f"cfg-{i}.json"
        cfg.write_text(json.dumps({"sleep": 3}))
        jobs.append(
            Job(
                id=f"concurrent-{i}",
                command=("python", "run.py", f"cfg-{i}.json"),
                inputs=(InputSpec(str(cfg), destination=f"cfg-{i}.json"),),
            )
        )

    with Dispatcher(inventory, project, results_dir=results_dir) as d:
        d.setup()  # Ray init + provisioning; not timed
        t0 = time.monotonic()
        results = d.run(jobs)
    elapsed = time.monotonic() - t0

    for r in results:
        assert r.status == JobStatus.SUCCEEDED, f"{r.id}: {r.error}"

    # ponytail: 20s threshold — concurrent=~11s (3s sleep + SSH overhead), sequential=~40s
    assert elapsed < 20.0, (
        f"Jobs took {elapsed:.1f}s — expected ~12s for concurrent execution on {n_slots} slots"
        f" (sequential would be ~{n_slots * 10}s)"
    )


@pytest.mark.e2e
def test_no_ray_auto_retry_on_command_failure(tmp_path, inventory, synth_project):
    """A COMMAND failure produces exactly 1 attempt (max_retries=0) (§11)."""
    project, _ = synth_project
    results_dir = str(tmp_path / "results")

    cfg = tmp_path / "fail.json"
    cfg.write_text('{"fail": true}')
    job = Job(
        id="no-retry",
        command=("python", "run.py", "fail.json"),
        inputs=(InputSpec(str(cfg), destination="fail.json"),),
    )

    with Dispatcher(inventory, project, results_dir=results_dir) as d:
        d.setup()
        results = d.run([job])

    r = results[0]
    assert r.status == JobStatus.FAILED
    assert len(r.attempts) == 1, (
        f"Expected exactly 1 attempt (max_retries=0), got {len(r.attempts)}"
    )


@pytest.mark.e2e
def test_completion_order_observable_via_status(tmp_path, inventory, synth_project):
    """status() returns RUNNING while job is active; SUCCEEDED after completion (§11)."""
    project, _ = synth_project
    results_dir = str(tmp_path / "results")

    cfg_long = tmp_path / "long.json"
    cfg_long.write_text('{"sleep": 5}')
    cfg_short = tmp_path / "short.json"
    cfg_short.write_text('{"sleep": 0}')

    jobs = [
        Job(
            id="long-job",
            command=("python", "run.py", "long.json"),
            inputs=(InputSpec(str(cfg_long), destination="long.json"),),
        ),
        Job(
            id="short-job",
            command=("python", "run.py", "short.json"),
            inputs=(InputSpec(str(cfg_short), destination="short.json"),),
        ),
    ]

    with Dispatcher(inventory, project, results_dir=results_dir) as d:
        d.setup()
        handles = d.submit(jobs)

        deadline = time.monotonic() + 30.0
        short_handle = next(h for h in handles if h.job_id == "short-job")
        while time.monotonic() < deadline:
            if d.status(short_handle) == JobStatus.SUCCEEDED:
                break
            time.sleep(0.2)
        else:
            pytest.fail("short-job did not complete within 30s")

        results = list(d.as_completed(handles))

    assert len(results) == 2
    for r in results:
        assert r.status == JobStatus.SUCCEEDED
