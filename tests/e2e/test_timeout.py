"""Multipass e2e timeout tests — §8.1 timeout enforcement on real VMs."""

import json
import time

import pytest

from ray_dispatcher import Dispatcher, InputSpec, Job, JobStatus


@pytest.mark.e2e
def test_job_times_out_and_slot_is_released(tmp_path, inventory, synth_project):
    """A job that sleeps longer than timeout_s is killed; status=TIMED_OUT; slot released (§8.1)."""
    project, _ = synth_project
    results_dir = str(tmp_path / "results")

    cfg = tmp_path / "sleep.json"
    cfg.write_text(json.dumps({"sleep": 30}))  # sleeps 30s; timeout will fire first

    job = Job(
        id="will-timeout",
        command=("python", "run.py", "sleep.json"),
        inputs=(InputSpec(str(cfg), destination="sleep.json"),),
        timeout_s=3.0,
    )

    with Dispatcher(inventory, project, results_dir=results_dir) as d:
        d.setup()
        t0 = time.monotonic()  # after setup — Ray startup adds ~12s
        results = d.run([job])
    elapsed = time.monotonic() - t0

    r = results[0]
    assert r.status == JobStatus.TIMED_OUT, f"expected TIMED_OUT, got {r.status}: {r.error}"
    assert r.returncode is None
    # Elapsed should be close to timeout_s (3s) + SSH overhead (~8-15s), well below 30s sleep.
    assert elapsed < 20.0, f"Timeout took too long: {elapsed:.1f}s"


@pytest.mark.e2e
def test_slot_available_after_timeout(tmp_path, inventory, synth_project):
    """After a timed-out job, the VM slot is released for a subsequent job."""
    project, _ = synth_project
    results_dir = str(tmp_path / "results")

    cfg_hang = tmp_path / "hang.json"
    cfg_hang.write_text(json.dumps({"sleep": 30}))

    # Fill all slots with hanging jobs.
    n_slots = sum(h.slots for h in inventory.hosts)
    hanging_jobs = [
        Job(
            id=f"hang-{i}",
            command=("python", "run.py", "hang.json"),
            inputs=(InputSpec(str(cfg_hang), destination="hang.json"),),
            timeout_s=3.0,
        )
        for i in range(n_slots)
    ]
    # One more quick job that should run after the hanging ones are killed.
    quick_job = Job(id="quick-after", command=("python", "run.py"))

    with Dispatcher(inventory, project, results_dir=results_dir) as d:
        d.setup()

        # Submit hanging jobs (will time out).
        hang_handles = d.submit(hanging_jobs, batch_id="batch-hang")

        # Drain them (all will TIMED_OUT).
        timed_out = list(d.as_completed(hang_handles))
        assert all(r.status == JobStatus.TIMED_OUT for r in timed_out)

        # Now submit and run the quick job — slot must be free.
        t0 = time.monotonic()
        results = d.run([quick_job], batch_id="batch-quick")
        elapsed = time.monotonic() - t0

    assert results[0].status == JobStatus.SUCCEEDED
    # ponytail: generous ceiling — quick job has no sleep, just SSH overhead
    assert elapsed < 60.0, f"Quick job waited too long for a slot: {elapsed:.1f}s"
