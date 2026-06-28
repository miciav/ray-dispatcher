from ray_dispatcher.models import Project, RemoteHost
from ray_dispatcher.provisioning import HostProvisioner, RemoteLayout
from ray_dispatcher.ssh import FakeTransport


def _prov(tmp_path):
    (tmp_path / "run.py").write_text("print('x')")
    p = HostProvisioner(
        FakeTransport(),  # default rc 0
        Project(path=str(tmp_path), project_id="dfaas", python="3.10.18", uv_version="0.11.25"),
        RemoteHost(host="10.0.0.1", user="ubuntu"),
        runner_path="x", session_id="s",
    )
    p.layout = RemoteLayout("/home/ubuntu", "dfaas")
    return p


def _scripts(t):
    return [a[2] for a in (c[1] for c in t.calls if c[0] == "run") if a[0] == "sh"]


def test_sync_source_pushes_with_delete_and_excludes(tmp_path):
    p = _prov(tmp_path)
    digest = p._sync_source()
    pushes = [c[1] for c in p.t.calls if c[0] == "push"]
    assert len(pushes) == 1
    local, remote, delete, excludes = pushes[0]
    assert local.endswith("/")  # trailing slash -> copy contents
    assert remote.endswith("/source.staging/")
    assert delete is True
    assert tuple(excludes) == (".venv/", ".git/", "solutions/")
    assert len(digest) == 64


def test_sync_source_atomically_replaces_then_writes_manifest(tmp_path):
    p = _prov(tmp_path)
    p._sync_source()
    scripts = _scripts(p.t)
    replace = [s for s in scripts if "mv" in s and "/source.staging" in s][0]
    assert "/source.staging" in replace and "mv" in replace
    manifest = [s for s in scripts if "source-manifest.json.tmp" in s][0]
    assert "source_digest" in manifest
    # manifest is written AFTER the atomic replace
    assert scripts.index(manifest) > scripts.index(replace)
