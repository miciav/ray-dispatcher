import pytest

from ray_dispatcher.backends import ssh_ray
from ray_dispatcher.dispatcher import Dispatcher
from ray_dispatcher.errors import BatchExistsError
from ray_dispatcher.models import (
    HostProvisioningResult,
    Inventory,
    Job,
    JobStatus,
    Project,
    ProvisioningReport,
    RemoteHost,
)
from ray_dispatcher.provisioning import ProvisioningOutcome
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _project():
    return Project(path="/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25")


def _canned_outcome(*host_names: str) -> ProvisioningOutcome:
    report = ProvisioningReport(tuple(
        HostProvisioningResult(h, True, "src123", "env123") for h in host_names
    ))
    return ProvisioningOutcome(report, sessions={})


def test_dispatcher_run_succeeds_end_to_end(tmp_path, monkeypatch):
    """Full stack: Dispatcher.run → SshRayBackend → _attempt_task → FakeTransport."""
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=2),))
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: _canned_outcome("10.0.0.1"))

    from ray_dispatcher.backends.ssh_ray import SshRayBackend

    # ponytail: inline closure so cloudpickle serializes without a module reference
    def _ok_transport(host: RemoteHost) -> FakeTransport:
        def results(argv: list[str]) -> CommandResult:
            cmd = " ".join(argv)
            if 'printf %s "$HOME"' in cmd:
                return CommandResult(0, "/home/ubuntu", "", 0.0)
            if argv[0] == "cat":
                return CommandResult(0, '{"returncode": 0, "duration_s": 0.05}', "", 0.0)
            return CommandResult(0, "", "", 0.0)
        return FakeTransport(run_results=results)

    backend = SshRayBackend(transport_factory=_ok_transport, results_dir=str(tmp_path))

    jobs = [Job(id=f"job-{i}", command=("echo", str(i))) for i in range(3)]
    with Dispatcher(inv, _project(), backend=backend, results_dir=str(tmp_path)) as d:
        d.setup()
        results = d.run(jobs)

    assert len(results) == 3
    assert [r.id for r in results] == [j.id for j in jobs]  # input order
    for r in results:
        assert r.status == JobStatus.SUCCEEDED


def test_dispatcher_run_raises_batch_failed_when_raise_on_failure(tmp_path, monkeypatch):
    """raise_on_failure=True raises BatchFailedError after all jobs complete."""
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=1),))
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: _canned_outcome("10.0.0.1"))

    from ray_dispatcher.backends.ssh_ray import SshRayBackend
    from ray_dispatcher.errors import BatchFailedError

    def _fail_transport(host: RemoteHost) -> FakeTransport:
        def results(argv: list[str]) -> CommandResult:
            if 'printf %s "$HOME"' in " ".join(argv):
                return CommandResult(0, "/home/ubuntu", "", 0.0)
            if argv[0] == "cat":
                return CommandResult(0, '{"returncode": 1, "duration_s": 0.05}', "", 0.0)
            return CommandResult(0, "", "", 0.0)
        return FakeTransport(run_results=results)

    backend = SshRayBackend(transport_factory=_fail_transport, results_dir=str(tmp_path))
    with pytest.raises(BatchFailedError) as exc_info:
        with Dispatcher(inv, _project(), backend=backend, raise_on_failure=True,
                        results_dir=str(tmp_path)) as d:
            d.setup()
            d.run([Job(id="j1", command=("fail",))])
    assert len(exc_info.value.results) == 1


def test_dispatcher_submit_raises_batch_exists_error(tmp_path, monkeypatch):
    """BatchExistsError raised if batch dir already exists (§4.5)."""
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=1),))
    monkeypatch.setattr(ssh_ray, "provision", lambda *a, **k: _canned_outcome("10.0.0.1"))

    from ray_dispatcher.backends.ssh_ray import SshRayBackend

    # ponytail: inline closure so cloudpickle serializes without a module reference
    def _ok_transport(host: RemoteHost) -> FakeTransport:
        def results(argv: list[str]) -> CommandResult:
            cmd = " ".join(argv)
            if 'printf %s "$HOME"' in cmd:
                return CommandResult(0, "/home/ubuntu", "", 0.0)
            if argv[0] == "cat":
                return CommandResult(0, '{"returncode": 0, "duration_s": 0.05}', "", 0.0)
            return CommandResult(0, "", "", 0.0)
        return FakeTransport(run_results=results)

    backend = SshRayBackend(transport_factory=_ok_transport, results_dir=str(tmp_path))

    batch_id = "existing-batch"
    (tmp_path / batch_id).mkdir()
    with Dispatcher(inv, _project(), backend=backend, results_dir=str(tmp_path)) as d:
        d.setup()
        with pytest.raises(BatchExistsError):
            d.submit([Job(id="j1", command=("echo",))], batch_id=batch_id)
