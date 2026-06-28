import pytest

from ray_dispatcher.models import Project, RemoteHost
from ray_dispatcher.provisioning import HostProvisioner, RemoteLayout, _StepError
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _prov(results):
    p = HostProvisioner(
        FakeTransport(run_results=results),
        Project(path="/local/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25"),
        RemoteHost(host="10.0.0.1", user="ubuntu"),
        runner_path="x", session_id="s",
    )
    p.layout = RemoteLayout("/home/ubuntu", "dfaas")
    return p


UV = "/home/ubuntu/.ray_dispatcher/uv/0.11.25/uv"
INTERP = "/home/ubuntu/.local/share/uv/python/cpython-3.10.18/bin/python3"


def test_install_python_installs_finds_and_verifies():
    def results(argv):
        if argv[:3] == [UV, "python", "install"]:
            return CommandResult(0, "", "", 0.0)
        if argv[:3] == [UV, "python", "find"]:
            return CommandResult(0, INTERP + "\n", "", 0.0)
        if argv[0] == INTERP:
            return CommandResult(0, "3.10.18\n", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    p = _prov(results)
    p._install_python(UV)  # no raise
    argvs = [c[1] for c in p.t.calls if c[0] == "run"]
    assert (UV, "python", "install", "3.10.18") in argvs


def test_install_python_version_mismatch_raises():
    def results(argv):
        if argv[:3] == [UV, "python", "find"]:
            return CommandResult(0, INTERP, "", 0.0)
        if argv[0] == INTERP:
            return CommandResult(0, "3.10.5\n", "", 0.0)  # wrong patch
        return CommandResult(0, "", "", 0.0)

    with pytest.raises(_StepError, match="3.10.18"):
        _prov(results)._install_python(UV)
