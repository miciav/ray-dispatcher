import subprocess

import pytest

from ray_dispatcher import ssh
from ray_dispatcher.ssh import SshConfig, SshTransport, TransportError


def _cfg():
    return SshConfig(host="h", user="u", port=22, identity_file=None,
                     known_hosts_file="/kh")


def test_push_builds_remote_dst_and_runs_rsync(monkeypatch):
    seen = {}

    def fake_run(argv, **kw):
        seen["argv"] = argv
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(ssh.subprocess, "run", fake_run)
    SshTransport(_cfg()).push("/local/dir", "/remote/dir", delete=True, excludes=(".git/",))
    assert seen["argv"][0] == "rsync"
    assert seen["argv"][-2:] == ["/local/dir", "u@h:/remote/dir"]
    assert "--delete" in seen["argv"]


def test_pull_builds_remote_src(monkeypatch):
    seen = {}
    monkeypatch.setattr(ssh.subprocess, "run",
                        lambda argv, **kw: seen.update(argv=argv) or
                        subprocess.CompletedProcess(argv, 0, "", ""))
    SshTransport(_cfg()).pull("/remote/out", "/local/out")
    assert seen["argv"][-2:] == ["u@h:/remote/out", "/local/out"]


def test_rsync_failure_raises_transport_error(monkeypatch):
    def boom(argv, **kw):
        raise subprocess.CalledProcessError(23, argv, stderr="rsync: link failed")

    monkeypatch.setattr(ssh.subprocess, "run", boom)
    with pytest.raises(TransportError):
        SshTransport(_cfg()).push("/a", "/b")
