import pytest

from ray_dispatcher.errors import NoHealthyHostsError, ProvisioningError
from ray_dispatcher.models import Inventory, Project, RemoteHost
from ray_dispatcher.provisioning import ProvisioningOutcome, provision
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _project(tmp_path):
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n")
    (tmp_path / "uv.lock").write_text("# lock\n")
    (tmp_path / "run.py").write_text("print('x')")
    return Project(path=str(tmp_path), project_id="dfaas", python="3.10.18", uv_version="0.11.25")


def _ok(argv):
    joined = " ".join(argv)
    if "$HOME" in joined:
        return CommandResult(0, "/home/ubuntu", "", 0.0)
    if argv[:2] == ["command", "-v"]:
        return CommandResult(0, "/usr/bin/x", "", 0.0)
    if "df -Pk" in joined:
        return CommandResult(0, "/dev/sda1 100 1 999999999 1% /\n", "", 0.0)
    if argv[-1] == "--version":
        return CommandResult(0, "uv 0.11.25", "", 0.0)
    if argv[1:3] == ["python", "find"]:
        return CommandResult(0, "/interp", "", 0.0)
    if argv[0] == "/interp":
        return CommandResult(0, "3.10.18", "", 0.0)
    if argv[0] == "uname":
        return CommandResult(0, "Linux x86_64", "", 0.0)
    return CommandResult(0, "", "", 0.0)


def _fail(argv):
    if argv[:2] == ["command", "-v"]:
        return CommandResult(1, "", "tool missing", 0.0)
    return _ok(argv)


def _runner(tmp_path):
    r = tmp_path / "remote_runner.py"
    r.write_text("print('r')")
    return str(r)


def test_provision_all_healthy_keeps_sessions(tmp_path):
    inv = Inventory((RemoteHost(host="a", user="ubuntu"), RemoteHost(host="b", user="ubuntu")))
    outcome = provision(
        inv, _project(tmp_path), runner_path=_runner(tmp_path),
        transport_factory=lambda h: FakeTransport(run_results=_ok),
    )
    assert isinstance(outcome, ProvisioningOutcome)
    assert [r.host for r in outcome.report.hosts] == ["a", "b"]  # inventory order
    assert all(r.succeeded for r in outcome.report.hosts)
    assert set(outcome.sessions) == {"ubuntu@a:22", "ubuntu@b:22"}
    outcome.release_all()  # cleanup heartbeats
    assert outcome.sessions == {}


def test_provision_partial_failure_proceeds_with_healthy(tmp_path):
    inv = Inventory((RemoteHost(host="good", user="ubuntu"), RemoteHost(host="bad", user="ubuntu")))

    def factory(h):
        return FakeTransport(run_results=_ok if h.host == "good" else _fail)

    outcome = provision(inv, _project(tmp_path), runner_path=_runner(tmp_path),
                        transport_factory=factory)
    assert [r.host for r in outcome.report.hosts] == ["good", "bad"]  # inventory order preserved
    by_host = {r.host: r for r in outcome.report.hosts}
    assert by_host["good"].succeeded and not by_host["bad"].succeeded
    assert set(outcome.sessions) == {"ubuntu@good:22"}  # only the healthy host kept
    outcome.release_all()


def test_require_all_hosts_aborts_and_releases(tmp_path):
    inv = Inventory((RemoteHost(host="good", user="ubuntu"), RemoteHost(host="bad", user="ubuntu")))

    def factory(h):
        return FakeTransport(run_results=_ok if h.host == "good" else _fail)

    with pytest.raises(ProvisioningError) as ei:
        provision(inv, _project(tmp_path), runner_path=_runner(tmp_path),
                  transport_factory=factory, require_all_hosts=True)
    assert ei.value.report is not None
    assert len(ei.value.report.hosts) == 2  # complete report collected before abort


def test_no_healthy_hosts_raises(tmp_path):
    inv = Inventory((RemoteHost(host="bad", user="ubuntu"),))
    with pytest.raises(NoHealthyHostsError):
        provision(inv, _project(tmp_path), runner_path=_runner(tmp_path),
                  transport_factory=lambda h: FakeTransport(run_results=_fail))


def test_provision_unreachable_host_does_not_crash_and_keeps_healthy(tmp_path):
    # a host whose transport raises on use must be marked unavailable, NOT crash the run,
    # and must not strand the healthy host's lock/heartbeat.
    inv = Inventory((
        RemoteHost(host="good", user="ubuntu"),
        RemoteHost(host="down", user="ubuntu"),
    ))

    def factory(h):
        if h.host == "good":
            return FakeTransport(run_results=_ok)

        def boom(argv):
            raise RuntimeError("connection refused")

        return FakeTransport(run_results=boom)

    outcome = provision(inv, _project(tmp_path), runner_path=_runner(tmp_path),
                        transport_factory=factory)  # must NOT raise
    by_host = {r.host: r for r in outcome.report.hosts}
    assert by_host["good"].succeeded and not by_host["down"].succeeded
    assert "connection refused" in by_host["down"].error
    assert set(outcome.sessions) == {"ubuntu@good:22"}
    outcome.release_all()
