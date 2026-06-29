import pytest

from ray_dispatcher.models import Project, RemoteHost
from ray_dispatcher.provisioning import HostProvisioner, RemoteLayout, _StepError
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _prov(results, **kw):
    p = HostProvisioner(
        FakeTransport(run_results=results),
        Project(path="/local/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25"),
        RemoteHost(host="10.0.0.1", user="ubuntu"),
        runner_path="x", session_id="s", **kw,
    )
    p.layout = RemoteLayout("/home/ubuntu", "dfaas")
    return p


def _df(avail_kb):
    return f"/dev/sda1 100000000 1 {avail_kb} 1% /\n"


def test_preflight_ok():
    def results(argv):
        if argv[:2] == ["command", "-v"]:
            return CommandResult(0, f"/usr/bin/{argv[2]}", "", 0.0)
        if argv[0] == "sh" and "df -Pk" in argv[2]:
            return CommandResult(0, _df(10_000_000), "", 0.0)  # ~9.5 GB
        return CommandResult(0, "", "", 0.0)

    _prov(results)._preflight()  # no raise


def test_preflight_missing_tool_raises():
    def results(argv):
        if argv == ["command", "-v", "rsync"]:
            return CommandResult(1, "", "", 0.0)
        if argv[:2] == ["command", "-v"]:
            return CommandResult(0, "/usr/bin/python3", "", 0.0)
        return CommandResult(0, _df(10_000_000), "", 0.0)

    with pytest.raises(_StepError, match="rsync"):
        _prov(results)._preflight()


def test_preflight_low_disk_raises():
    def results(argv):
        if argv[:2] == ["command", "-v"]:
            return CommandResult(0, "/usr/bin/x", "", 0.0)
        if argv[0] == "sh" and "df -Pk" in argv[2]:
            return CommandResult(0, _df(1000), "", 0.0)  # < 1 MB
        return CommandResult(0, "", "", 0.0)

    with pytest.raises(_StepError, match="disk"):
        _prov(results, min_disk_mb=500)._preflight()
