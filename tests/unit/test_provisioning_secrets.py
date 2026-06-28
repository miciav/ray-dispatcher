import pytest

from ray_dispatcher.models import Project, RemoteHost, SecretFile
from ray_dispatcher.provisioning import HostProvisioner, RemoteLayout, _StepError
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _prov(results, secrets, *, user="ubuntu"):
    p = HostProvisioner(
        FakeTransport(run_results=results),
        Project(path="/local/proj", project_id="dfaas", python="3.10.18",
                uv_version="0.11.25", secrets=secrets),
        RemoteHost(host="10.0.0.1", user=user),
        runner_path="x", session_id="s",
    )
    p.layout = RemoteLayout("/home/ubuntu", "dfaas")
    return p


def _scripts(t):
    return [a[2] for a in (c[1] for c in t.calls if c[0] == "run") if a[0] == "sh"]


def test_copy_secrets_noop_when_none():
    p = _prov(lambda a: CommandResult(0, "", "", 0.0), secrets=())
    p._copy_secrets()
    assert not p.t.calls


def test_copy_secrets_pushes_chmods_and_verifies_owner():
    def results(argv):
        if argv[0] == "sh" and "stat" in argv[2]:
            return CommandResult(0, "ubuntu\n", "", 0.0)  # owner matches
        return CommandResult(0, "", "", 0.0)

    secrets = (SecretFile(source="/local/token", remote_name="token", mode=0o600),)
    p = _prov(results, secrets=secrets)
    p._copy_secrets()
    pushes = [c[1] for c in p.t.calls if c[0] == "push"]
    assert pushes[0][1] == "/home/ubuntu/.ray_dispatcher/secrets/dfaas/token"
    chmods = [c[1] for c in p.t.calls if c[0] == "run" and c[1][0] == "chmod"]
    assert ("chmod", "600", "/home/ubuntu/.ray_dispatcher/secrets/dfaas/token") == \
        (chmods[0][0], chmods[0][1], chmods[0][2])
    assert any("mkdir -p" in s and "chmod 700" in s for s in _scripts(p.t))  # 0700 dir
    # contents are never printed: no `cat`/`printf` of the secret file itself
    assert not any("cat /home/ubuntu/.ray_dispatcher/secrets" in s for s in _scripts(p.t))
    assert any("stat" in s for s in _scripts(p.t))  # ownership was actually verified


def test_copy_secrets_wrong_owner_raises():
    def results(argv):
        if argv[0] == "sh" and "stat" in argv[2]:
            return CommandResult(0, "root\n", "", 0.0)  # owner mismatch
        return CommandResult(0, "", "", 0.0)

    secrets = (SecretFile(source="/local/token", remote_name="token"),)
    with pytest.raises(_StepError, match="owned by"):
        _prov(results, secrets=secrets, user="ubuntu")._copy_secrets()


def test_copy_secrets_unverifiable_owner_raises():
    # an empty/unreadable owner must fail closed, not silently skip verification
    def results(argv):
        if argv[0] == "sh" and "stat" in argv[2]:
            return CommandResult(0, "\n", "", 0.0)  # empty owner
        return CommandResult(0, "", "", 0.0)

    secrets = (SecretFile(source="/local/token", remote_name="token"),)
    with pytest.raises(_StepError, match="owned by"):
        _prov(results, secrets=secrets)._copy_secrets()
