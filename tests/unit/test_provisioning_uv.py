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


def _scripts(t):
    return [a[2] for a in (c[1] for c in t.calls if c[0] == "run") if a[0] == "sh"]


def test_install_uv_skips_when_present_and_correct():
    def results(argv):
        if argv[-1] == "--version":
            return CommandResult(0, "uv 0.11.25", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    p = _prov(results)
    uv = p._install_uv()
    assert uv == "/home/ubuntu/.ray_dispatcher/uv/0.11.25/uv"
    assert not any("astral.sh" in s for s in _scripts(p.t))  # no download


def test_install_uv_downloads_versioned_installer_then_verifies():
    state = {"installed": False}

    def results(argv):
        if argv[-1] == "--version":
            if state["installed"]:
                return CommandResult(0, "uv 0.11.25", "", 0.0)
            return CommandResult(1, "", "not found", 0.0)
        if argv[0] == "sh" and "astral.sh/uv/0.11.25/install.sh" in argv[2]:
            state["installed"] = True
            return CommandResult(0, "", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    p = _prov(results)
    p._install_uv()
    dl = [s for s in _scripts(p.t) if "astral.sh/uv/0.11.25/install.sh" in s][0]
    assert "UV_INSTALL_DIR=" in dl and "/uv/0.11.25" in dl and "INSTALLER_NO_MODIFY_PATH=1" in dl


def test_install_uv_force_redownloads_even_if_present():
    seen = {"download": False}

    def results(argv):
        if argv[-1] == "--version":
            return CommandResult(0, "uv 0.11.25", "", 0.0)
        if argv[0] == "sh" and "astral.sh" in argv[2]:
            seen["download"] = True
        return CommandResult(0, "", "", 0.0)

    _prov(results, force=True)._install_uv()
    assert seen["download"] is True


def test_install_uv_wrong_version_after_install_raises():
    def results(argv):
        if argv[-1] == "--version":
            return CommandResult(0, "uv 0.9.0", "", 0.0)  # never the wanted version
        return CommandResult(0, "", "", 0.0)

    with pytest.raises(_StepError, match="expected 0.11.25"):
        _prov(results, force=True)._install_uv()


def test_install_uv_does_not_skip_on_prefix_version_match():
    # exact pin: requesting 0.11.2 must NOT be satisfied by an installed 0.11.25
    state = {"downloaded": False}

    def results(argv):
        if argv[-1] == "--version":
            return CommandResult(0, "uv 0.11.25", "", 0.0)  # a different, longer version
        if argv[0] == "sh" and "astral.sh" in argv[2]:
            state["downloaded"] = True
            return CommandResult(0, "", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    p = HostProvisioner(
        FakeTransport(run_results=results),
        Project(path="/local/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.2"),
        RemoteHost(host="10.0.0.1", user="ubuntu"),
        runner_path="x", session_id="s",
    )
    p.layout = RemoteLayout("/home/ubuntu", "dfaas")
    with pytest.raises(_StepError, match="expected 0.11.2"):
        p._install_uv()
    assert state["downloaded"] is True  # it did NOT skip — it re-downloaded
