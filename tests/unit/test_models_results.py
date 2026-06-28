import pytest

from ray_dispatcher.errors import ModelValidationError
from ray_dispatcher.models import (
    AttemptResult,
    FailureKind,
    HostProvisioningResult,
    JobHandle,
    JobResult,
    JobStatus,
    ProvisioningReport,
    RetryPolicy,
)


def test_job_status_values():
    assert {s.value for s in JobStatus} == {
        "pending", "running", "succeeded", "failed", "timed_out", "cancelled",
    }


def test_failure_kind_values():
    assert {k.value for k in FailureKind} == {
        "command", "ssh", "timeout", "output_missing", "collection", "host_lost",
        "internal",
    }


def test_retry_policy_defaults():
    policy = RetryPolicy()
    assert policy.max_attempts == 2
    assert policy.retry_on == frozenset(
        {FailureKind.SSH, FailureKind.HOST_LOST, FailureKind.COLLECTION}
    )


def test_retry_policy_rejects_zero_attempts():
    with pytest.raises(ModelValidationError):
        RetryPolicy(max_attempts=0)


def test_job_handle_is_hashable():
    h = JobHandle(batch_id="b", job_id="j", token="t")
    assert h in {h}


def test_result_objects_construct():
    attempt = AttemptResult(
        number=1, host="h", status=JobStatus.SUCCEEDED, returncode=0,
        duration_s=1.0, stdout_log="o", stderr_log="e",
    )
    result = JobResult(
        id="j", batch_id="b", status=JobStatus.SUCCEEDED, returncode=0,
        duration_s=1.0, host="h", output_dir="./results/b/j/outputs",
        attempts=(attempt,),
    )
    assert result.attempts[0].failure_kind is None
    report = ProvisioningReport(
        hosts=(HostProvisioningResult(host="h", succeeded=True,
                                      source_digest="s", environment_digest="e"),)
    )
    assert report.hosts[0].succeeded is True
