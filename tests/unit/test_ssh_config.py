import pytest

from ray_dispatcher.errors import ModelValidationError
from ray_dispatcher.models import RemoteHost
from ray_dispatcher.ssh import SshConfig


def _host(tmp_path, **over):
    kh = tmp_path / "known_hosts"
    kh.write_text("")
    kwargs = dict(host="10.0.0.5", user="ubuntu", known_hosts_file=str(kh))
    kwargs.update(over)
    return RemoteHost(**kwargs)


def test_from_host_resolves_paths(tmp_path):
    idf = tmp_path / "id_ed25519"
    idf.write_text("key")
    cfg = SshConfig.from_host(_host(tmp_path, identity_file=str(idf)))
    assert cfg.host == "10.0.0.5"
    assert cfg.user == "ubuntu"
    assert cfg.port == 22
    assert cfg.identity_file == str(idf.resolve())
    assert cfg.known_hosts_file.endswith("known_hosts")


def test_from_host_allows_no_identity_uses_agent(tmp_path):
    cfg = SshConfig.from_host(_host(tmp_path))  # identity_file=None
    assert cfg.identity_file is None


def test_from_host_rejects_missing_identity(tmp_path):
    with pytest.raises(ModelValidationError):
        SshConfig.from_host(_host(tmp_path, identity_file=str(tmp_path / "nope")))


def test_from_host_rejects_missing_known_hosts(tmp_path):
    h = RemoteHost(host="h", user="u", known_hosts_file=str(tmp_path / "absent"))
    with pytest.raises(ModelValidationError):
        SshConfig.from_host(h)
