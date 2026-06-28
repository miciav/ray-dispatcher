import paramiko

from ray_dispatcher.ssh import SshConfig, build_connection


def test_build_connection_sets_host_user_port_and_identity(tmp_path):
    kh = tmp_path / "known_hosts"
    kh.write_text("")
    cfg = SshConfig(host="10.0.0.5", user="ubuntu", port=2222,
                    identity_file="/keys/id", known_hosts_file=str(kh))
    conn = build_connection(cfg)
    assert conn.host == "10.0.0.5"
    assert conn.user == "ubuntu"
    assert conn.port == 2222
    # Fabric 3.x normalises key_filename to a list internally
    assert conn.connect_kwargs["key_filename"] == ["/keys/id"]
    # host-key checking is enforced via a RejectPolicy on the paramiko client
    assert isinstance(conn.client._policy, paramiko.RejectPolicy)


def test_build_connection_without_identity_uses_agent(tmp_path):
    kh = tmp_path / "known_hosts"
    kh.write_text("")
    cfg = SshConfig(host="h", user="u", port=22, identity_file=None,
                    known_hosts_file=str(kh))
    conn = build_connection(cfg)
    assert "key_filename" not in conn.connect_kwargs
