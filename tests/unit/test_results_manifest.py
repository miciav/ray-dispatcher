import json

from ray_dispatcher.models import AttemptResult, FailureKind, JobResult, JobStatus
from ray_dispatcher.results import write_attempt_json, write_result_json


def _attempt(n=1, status=JobStatus.SUCCEEDED, fk=None):
    return AttemptResult(
        number=n,
        host="10.0.0.1",
        status=status,
        returncode=0 if status is JobStatus.SUCCEEDED else 1,
        duration_s=1.5,
        stdout_log="attempts/1/stdout.log",
        stderr_log="attempts/1/stderr.log",
        failure_kind=fk,
        error=None,
    )


def test_write_attempt_json_serializes_enums_as_values(tmp_path):
    p = tmp_path / "attempt.json"
    write_attempt_json(p, _attempt(status=JobStatus.FAILED, fk=FailureKind.COMMAND),
                       missing_optional=("logs/extra.txt",))
    doc = json.loads(p.read_text())
    assert doc["number"] == 1
    assert doc["status"] == "failed"               # enum -> .value
    assert doc["failure_kind"] == "command"        # enum -> .value
    assert doc["returncode"] == 1
    assert doc["missing_optional"] == ["logs/extra.txt"]


def test_write_attempt_json_null_failure_kind(tmp_path):
    p = tmp_path / "attempt.json"
    write_attempt_json(p, _attempt())              # success: no failure_kind
    doc = json.loads(p.read_text())
    assert doc["status"] == "succeeded"
    assert doc["failure_kind"] is None
    assert doc["missing_optional"] == []           # default empty


def test_write_result_json_includes_nested_attempts(tmp_path):
    p = tmp_path / "result.json"
    result = JobResult(
        id="jobA",
        batch_id="batch1",
        status=JobStatus.SUCCEEDED,
        returncode=0,
        duration_s=2.0,
        host="10.0.0.1",
        output_dir="results/batch1/jobA/outputs",
        attempts=(_attempt(),),
        error=None,
    )
    write_result_json(p, result)
    doc = json.loads(p.read_text())
    assert doc["id"] == "jobA"
    assert doc["status"] == "succeeded"
    assert isinstance(doc["attempts"], list) and len(doc["attempts"]) == 1
    assert doc["attempts"][0]["status"] == "succeeded"
    assert doc["output_dir"].endswith("/outputs")
