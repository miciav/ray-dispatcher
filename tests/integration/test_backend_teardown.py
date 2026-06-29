import ray

from ray_dispatcher.backends import ssh_ray
from ray_dispatcher.backends.ssh_ray import SshRayBackend
from ray_dispatcher.models import (
    HostProvisioningResult,
    Inventory,
    Project,
    ProvisioningReport,
    RemoteHost,
)
from ray_dispatcher.provisioning import ProvisioningOutcome
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _project():
    return Project(path="/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25")


def _home_transport(host):
    def results(argv):
        if 'printf %s "$HOME"' in " ".join(argv):
            return CommandResult(0, "/home/ubuntu", "", 0.0)
        return CommandResult(0, "", "", 0.0)
    return FakeTransport(run_results=results)


class _Recorder:
    """Stands in for both the session lock and its heartbeat thread.

    `ProvisioningOutcome.release_all()` calls `hb.stop()` then `lock.release()`;
    recording both lets the test prove release_all actually iterated, not just
    that the dict was reassigned to {}.
    """

    def __init__(self):
        self.calls = []

    def stop(self):
        self.calls.append("stop")

    def release(self):
        self.calls.append("release")


def _canned_outcome(host_name, session):
    report = ProvisioningReport((HostProvisioningResult(host_name, True, "src123", "env123"),))
    return ProvisioningOutcome(report, sessions={host_name: (session, session)})


def test_teardown_shuts_down_owned_runtime_and_releases_locks(monkeypatch):
    inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu", slots=1),))
    rec = _Recorder()
    monkeypatch.setattr(ssh_ray, "provision",
                        lambda *a, **k: _canned_outcome("10.0.0.1", rec))
    backend = SshRayBackend(transport_factory=_home_transport)
    backend.setup(inv, _project())
    assert ray.is_initialized()
    backend.teardown()
    assert not ray.is_initialized()         # owned runtime shut down (§10.5)
    assert rec.calls == ["stop", "release"]  # release_all() ran: hb stopped, lock released (§10.4)
    assert backend._outcome.sessions == {}   # sessions cleared


def test_teardown_is_safe_without_setup():
    backend = SshRayBackend(transport_factory=_home_transport)
    backend.teardown()  # no runtime owned -> no error, no ray.shutdown of a foreign runtime
