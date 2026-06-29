import pytest

from ray_dispatcher.models import Project, RemoteHost
from ray_dispatcher.provisioning import HostProvisioner, RemoteLayout, _StepError
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _runs(t):
    return [c[1] for c in t.calls if c[0] == "run"]


def _project():
    return Project(path="/local/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25")


def _host():
    return RemoteHost(host="10.0.0.1", user="ubuntu")


def test_layout_paths_are_absolute_under_home():
    lo = RemoteLayout("/home/ubuntu", "dfaas")
    assert lo.root == "/home/ubuntu/.ray_dispatcher"
    assert lo.source == "/home/ubuntu/.ray_dispatcher/projects/dfaas/source"
    assert lo.source_manifest == "/home/ubuntu/.ray_dispatcher/projects/dfaas/source-manifest.json"
    assert lo.secrets == "/home/ubuntu/.ray_dispatcher/secrets/dfaas"
    assert lo.env_dir("abc") == "/home/ubuntu/.ray_dispatcher/projects/dfaas/envs/abc"
    assert lo.env_venv("abc").endswith("/envs/abc/.venv")
    assert lo.env_manifest("abc").endswith("/envs/abc/environment-manifest.json")
    assert lo.runner("deadbeef").endswith("/bin/deadbeef/remote_runner.py")
    assert lo.uv_bin("0.11.25").endswith("/uv/0.11.25/uv")


def test_resolve_layout_probes_home():
    def results(argv):
        if argv[:2] == ["sh", "-c"] and "$HOME" in argv[2]:
            return CommandResult(0, "/home/ubuntu\n", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    t = FakeTransport(run_results=results)
    p = HostProvisioner(t, _project(), _host(), runner_path="x", session_id="s")
    lo = p._resolve_layout()
    assert lo.source == "/home/ubuntu/.ray_dispatcher/projects/dfaas/source"


def test_checked_raises_step_error_on_nonzero():
    def results(argv):
        return CommandResult(3, "", "boom", 0.0)

    t = FakeTransport(run_results=results)
    p = HostProvisioner(t, _project(), _host(), runner_path="x", session_id="s")
    with pytest.raises(_StepError, match="boom"):
        p._checked(["false"], "do thing")


def test_write_remote_file_is_atomic_tmp_then_mv():
    t = FakeTransport()  # default rc 0
    p = HostProvisioner(t, _project(), _host(), runner_path="x", session_id="s")
    p._write_remote_file(
        "/home/ubuntu/.ray_dispatcher/projects/dfaas/source-manifest.json", '{"a":1}'
    )
    script = _runs(t)[-1][2]
    assert "printf %s" in script and "source-manifest.json.tmp" in script and "mv -f" in script
