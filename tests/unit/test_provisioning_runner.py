from ray_dispatcher.models import Project, RemoteHost
from ray_dispatcher.provisioning import HostProvisioner, RemoteLayout
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _prov(tmp_path, results, **kw):
    runner = tmp_path / "remote_runner.py"
    runner.write_text("print('runner')")
    p = HostProvisioner(
        FakeTransport(run_results=results),
        Project(path="/local/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25"),
        RemoteHost(host="10.0.0.1", user="ubuntu"),
        runner_path=str(runner), session_id="s", **kw,
    )
    p.layout = RemoteLayout("/home/ubuntu", "dfaas")
    return p


def test_install_runner_pushes_when_absent(tmp_path):
    def results(argv):
        if argv[0] == "test" and argv[1] == "-f":
            return CommandResult(1, "", "", 0.0)  # absent
        return CommandResult(0, "", "", 0.0)

    p = _prov(tmp_path, results)
    digest = p._install_runner()
    pushes = [c[1] for c in p.t.calls if c[0] == "push"]
    assert len(pushes) == 1
    local, remote, _, _ = pushes[0]
    assert remote == f"/home/ubuntu/.ray_dispatcher/bin/{digest}/remote_runner.py"


def test_install_runner_skips_when_present(tmp_path):
    def results(argv):
        if argv[0] == "test" and argv[1] == "-f":
            return CommandResult(0, "", "", 0.0)  # present
        return CommandResult(0, "", "", 0.0)

    p = _prov(tmp_path, results)
    p._install_runner()
    assert not [c for c in p.t.calls if c[0] == "push"]


def test_install_runner_force_pushes_even_if_present(tmp_path):
    def results(argv):
        if argv[0] == "test" and argv[1] == "-f":
            return CommandResult(0, "", "", 0.0)
        return CommandResult(0, "", "", 0.0)

    p = _prov(tmp_path, results, force=True)
    p._install_runner()
    assert [c for c in p.t.calls if c[0] == "push"]
