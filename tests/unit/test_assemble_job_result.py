from ray_dispatcher.models import AttemptResult, FailureKind, JobStatus
from ray_dispatcher.scheduling import assemble_job_result


def _attempt(n, host, status, rc, dur, fk=None, err=None):
    return AttemptResult(
        number=n, host=host, status=status, returncode=rc, duration_s=dur,
        stdout_log=f"a/{n}/stdout.log", stderr_log=f"a/{n}/stderr.log",
        failure_kind=fk, error=err,
    )


def test_success_uses_final_attempt_and_sets_output_dir():
    attempts = [
        _attempt(1, "a", JobStatus.FAILED, None, 1.0, FailureKind.SSH, "ssh down"),
        _attempt(2, "b", JobStatus.SUCCEEDED, 0, 2.5),
    ]
    r = assemble_job_result("jobA", "b1", attempts, outputs_dir="/res/b1/jobA/outputs")
    assert r.status is JobStatus.SUCCEEDED
    assert r.host == "b"               # final attempt
    assert r.returncode == 0
    assert r.duration_s == 3.5         # sum across attempts
    assert r.output_dir == "/res/b1/jobA/outputs"
    assert len(r.attempts) == 2
    assert r.error is None


def test_failure_has_no_output_dir_and_keeps_final_error():
    attempts = [_attempt(1, "a", JobStatus.FAILED, 3, 1.0, FailureKind.COMMAND, "boom")]
    r = assemble_job_result("jobA", "b1", attempts, outputs_dir="/res/b1/jobA/outputs")
    assert r.status is JobStatus.FAILED
    assert r.host == "a"
    assert r.returncode == 3
    assert r.output_dir is None        # no publish on failure
    assert r.error == "boom"
    assert r.id == "jobA" and r.batch_id == "b1"
