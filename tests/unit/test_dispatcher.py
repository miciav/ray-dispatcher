import uuid
from unittest.mock import MagicMock

import pytest

from ray_dispatcher.backends.base import ExecutionBackend
from ray_dispatcher.dispatcher import Dispatcher
from ray_dispatcher.errors import (
    BatchExistsError,
    BatchFailedError,
    DispatcherError,
    ProvisioningError,
)
from ray_dispatcher.models import (
    HostProvisioningResult,
    Inventory,
    Job,
    JobHandle,
    JobResult,
    JobStatus,
    Project,
    ProvisioningReport,
    RemoteHost,
)


def _inv():
    return Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=1),))


def _proj():
    return Project(path="/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25")


def _report(succeeded: bool = True):
    return ProvisioningReport((HostProvisioningResult("10.0.0.1", succeeded, "src123", "env123"),))


def _mock_backend(report=None):
    b = MagicMock(spec=ExecutionBackend)
    b.setup.return_value = report or _report()
    b.submit.side_effect = lambda batch_id, job: JobHandle(
        batch_id=batch_id, job_id=job.id, token=uuid.uuid4().hex
    )
    b.status.return_value = JobStatus.SUCCEEDED
    b.resolve.side_effect = lambda h: JobResult(
        id=h.job_id, batch_id=h.batch_id, status=JobStatus.SUCCEEDED,
        returncode=0, duration_s=0.1, host="10.0.0.1", output_dir=None, attempts=(),
    )
    return b


def test_setup_calls_backend_setup():
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b)
    report = d.setup()
    b.setup.assert_called_once()
    assert report.hosts[0].succeeded


def test_setup_idempotent_returns_same_report():
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b)
    r1 = d.setup()
    r2 = d.setup()
    assert r1 is r2
    b.setup.assert_called_once()  # only called once


def test_setup_force_after_setup_raises():
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b)
    d.setup()
    with pytest.raises(DispatcherError, match="force"):
        d.setup(force=True)


def test_require_all_hosts_raises_on_partial_failure():
    b = _mock_backend(report=ProvisioningReport((
        HostProvisioningResult("10.0.0.1", True, "src123", "env123"),
        HostProvisioningResult("10.0.0.2", False, None, None),
    )))
    d = Dispatcher(_inv(), _proj(), backend=b, require_all_hosts=True)
    with pytest.raises(ProvisioningError):
        d.setup()


def test_require_all_hosts_false_allows_partial():
    b = _mock_backend(report=ProvisioningReport((
        HostProvisioningResult("10.0.0.1", True, "src123", "env123"),
        HostProvisioningResult("10.0.0.2", False, None, None),
    )))
    d = Dispatcher(_inv(), _proj(), backend=b, require_all_hosts=False)
    report = d.setup()
    assert report is not None


def test_submit_auto_sets_up(tmp_path):
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b, results_dir=str(tmp_path))
    job = Job(id="j1", command=("echo",))
    handles = d.submit([job])
    b.setup.assert_called_once()
    assert len(handles) == 1
    assert handles[0].job_id == "j1"


def test_submit_generates_batch_id_when_none(tmp_path):
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b, results_dir=str(tmp_path))
    handles = d.submit([Job(id="j1", command=("echo",))])
    assert handles[0].batch_id  # non-empty


def test_submit_raises_batch_exists_error(tmp_path):
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b, results_dir=str(tmp_path))
    batch_id = "mybatch"
    (tmp_path / batch_id).mkdir()
    with pytest.raises(BatchExistsError):
        d.submit([Job(id="j1", command=("echo",))], batch_id=batch_id)


def test_status_delegates_to_backend():
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b)
    d.setup()
    h = JobHandle(batch_id="b1", job_id="j1", token="tok1")
    s = d.status(h)
    b.status.assert_called_once_with(h)
    assert s == JobStatus.SUCCEEDED


def test_cancel_delegates_to_backend():
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b)
    d.setup()
    h = JobHandle(batch_id="b1", job_id="j1", token="tok1")
    d.cancel(h)
    b.cancel.assert_called_once_with(h)


def test_as_completed_yields_in_completion_order(tmp_path):
    b = _mock_backend()
    # Make status return RUNNING first, then SUCCEEDED on second call per handle
    call_counts: dict[str, int] = {}
    def _status(h):
        call_counts.setdefault(h.token, 0)
        call_counts[h.token] += 1
        return JobStatus.RUNNING if call_counts[h.token] < 2 else JobStatus.SUCCEEDED
    b.status.side_effect = _status

    d = Dispatcher(_inv(), _proj(), backend=b, results_dir=str(tmp_path))
    jobs = [Job(id=f"j{i}", command=("echo",)) for i in range(3)]
    handles = d.submit(jobs)
    results = list(d.as_completed(handles))
    assert len(results) == 3
    for r in results:
        assert r.status == JobStatus.SUCCEEDED


def test_run_returns_results_in_input_order(tmp_path):
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b, results_dir=str(tmp_path))
    jobs = [Job(id=f"job-{i}", command=("echo",)) for i in range(5)]
    results = d.run(jobs)
    assert [r.id for r in results] == [j.id for j in jobs]


def test_run_raises_batch_failed_error(tmp_path):
    b = _mock_backend()
    b.resolve.side_effect = lambda h: JobResult(
        id=h.job_id, batch_id=h.batch_id, status=JobStatus.FAILED,
        returncode=1, duration_s=0.0, host=None, output_dir=None, attempts=(),
    )
    d = Dispatcher(_inv(), _proj(), backend=b, raise_on_failure=True, results_dir=str(tmp_path))
    with pytest.raises(BatchFailedError) as exc:
        d.run([Job(id="j1", command=("fail",))])
    assert len(exc.value.results) == 1


def test_run_drains_all_before_raising(tmp_path):
    """raise_on_failure=True still drains all jobs before raising."""
    b = _mock_backend()
    resolved: list[str] = []
    def _resolve(h):
        resolved.append(h.job_id)
        return JobResult(id=h.job_id, batch_id=h.batch_id, status=JobStatus.FAILED,
                         returncode=1, duration_s=0.0, host=None, output_dir=None, attempts=())
    b.resolve.side_effect = _resolve
    d = Dispatcher(_inv(), _proj(), backend=b, raise_on_failure=True, results_dir=str(tmp_path))
    with pytest.raises(BatchFailedError):
        d.run([Job(id=f"j{i}", command=("fail",)) for i in range(3)])
    assert len(resolved) == 3


def test_teardown_cancels_outstanding_and_delegates(tmp_path):
    b = _mock_backend()
    d = Dispatcher(_inv(), _proj(), backend=b, results_dir=str(tmp_path))
    d.submit([Job(id="j1", command=("echo",))])
    d.teardown()
    b.cancel.assert_called()
    b.teardown.assert_called_once_with(purge=False)


def test_teardown_purge_rejected_when_active(tmp_path):
    b = _mock_backend()
    b.status.return_value = JobStatus.RUNNING
    d = Dispatcher(_inv(), _proj(), backend=b, results_dir=str(tmp_path))
    d.submit([Job(id="j1", command=("echo",))])
    with pytest.raises(DispatcherError, match="purge"):
        d.teardown(purge=True)


def test_context_manager_calls_teardown():
    b = _mock_backend()
    with Dispatcher(_inv(), _proj(), backend=b) as d:
        assert d is not None
    b.teardown.assert_called_once()


def test_context_manager_enter_does_no_network():
    b = _mock_backend()
    with Dispatcher(_inv(), _proj(), backend=b):
        pass
    b.setup.assert_not_called()  # __enter__ must not call setup


def test_context_manager_swallows_cleanup_error_when_exception_in_flight():
    b = _mock_backend()
    b.teardown.side_effect = RuntimeError("cleanup boom")
    with pytest.raises(ValueError, match="original"):
        with Dispatcher(_inv(), _proj(), backend=b):
            raise ValueError("original")
    # RuntimeError from teardown must NOT replace the ValueError
