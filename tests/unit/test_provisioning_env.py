from ray_dispatcher.models import Project, RemoteHost
from ray_dispatcher.provisioning import HostProvisioner, RemoteLayout
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _prov(tmp_path, results, *, groups=(), **kw):
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n")
    (tmp_path / "uv.lock").write_text("# lock\n")
    p = HostProvisioner(
        FakeTransport(run_results=results),
        Project(path=str(tmp_path), project_id="dfaas", python="3.10.18",
                uv_version="0.11.25", dependency_groups=groups),
        RemoteHost(host="10.0.0.1", user="ubuntu"),
        runner_path="x", session_id="s", **kw,
    )
    p.layout = RemoteLayout("/home/ubuntu", "dfaas")
    return p


UV = "/home/ubuntu/.ray_dispatcher/uv/0.11.25/uv"


def _scripts(t):
    return [a[2] for a in (c[1] for c in t.calls if c[0] == "run") if a[0] == "sh"]


def test_publish_env_skips_when_already_valid(tmp_path):
    def results(argv):
        if argv[0] == "uname":
            return CommandResult(0, "Linux x86_64\n", "", 0.0)
        if argv[0] == "sh" and "test -f" in argv[2] and "environment-manifest.json" in argv[2]:
            return CommandResult(0, "", "", 0.0)  # already published & valid
        return CommandResult(0, "", "", 0.0)

    p = _prov(tmp_path, results)
    p._publish_env(UV)
    assert not any("uv" in s and "sync" in s for s in _scripts(p.t))  # no sync ran


def test_publish_env_syncs_smoke_checks_and_publishes(tmp_path):
    def results(argv):
        if argv[0] == "uname":
            return CommandResult(0, "Linux x86_64\n", "", 0.0)
        if argv[0] == "sh" and "test -f" in argv[2] and "environment-manifest" in argv[2]:
            return CommandResult(1, "", "", 0.0)  # not yet valid -> build it
        return CommandResult(0, "", "", 0.0)

    p = _prov(tmp_path, results, groups=("dev",))
    digest = p._publish_env(UV)
    scripts = _scripts(p.t)
    sync = [s for s in scripts if "uv" in s and "sync" in s][0]
    assert "UV_PROJECT_ENVIRONMENT=" in sync
    assert "--locked" in sync and "--no-install-project" in sync and "--no-default-groups" in sync
    assert "--group dev" in sync
    assert "--python 3.10.18" in sync
    smoke = [s for s in scripts if "import sys" in s][0]
    publish = [s for s in scripts if "mv" in s and "/envs/" in s and ".staging" in s][0]
    # smoke check happens before the atomic publish
    assert scripts.index(smoke) < scripts.index(publish)
    manifest = [s for s in scripts if "environment-manifest.json.tmp" in s][0]
    assert digest in manifest


def test_publish_env_force_rebuilds_even_if_valid(tmp_path):
    def results(argv):
        if argv[0] == "uname":
            return CommandResult(0, "Linux x86_64\n", "", 0.0)
        if argv[0] == "sh" and "test -f" in argv[2] and "environment-manifest" in argv[2]:
            return CommandResult(0, "", "", 0.0)  # would normally skip
        return CommandResult(0, "", "", 0.0)

    p = _prov(tmp_path, results, force=True)
    p._publish_env(UV)
    assert any("uv" in s and "sync" in s for s in _scripts(p.t))  # rebuilt anyway
