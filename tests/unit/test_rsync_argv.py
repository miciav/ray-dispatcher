from ray_dispatcher.ssh import SshConfig, build_rsync_argv


def _cfg():
    return SshConfig(host="h", user="u", port=2222,
                     identity_file="/keys/id", known_hosts_file="/keys/kh")


def test_argv_has_archive_and_ssh_options():
    argv = build_rsync_argv(_cfg(), "/local/", "u@h:/remote/", delete=False, excludes=())
    assert argv[0] == "rsync"
    assert "-a" in argv
    # the -e value is one argument holding the ssh invocation
    e_index = argv.index("-e")
    ssh_opt = argv[e_index + 1]
    assert ssh_opt.startswith("ssh ")
    assert "-p 2222" in ssh_opt
    assert "-i /keys/id" in ssh_opt
    assert "UserKnownHostsFile=/keys/kh" in ssh_opt
    assert "StrictHostKeyChecking=yes" in ssh_opt
    assert "GlobalKnownHostsFile=/dev/null" in ssh_opt
    assert "BatchMode=yes" in ssh_opt
    assert argv[-2:] == ["/local/", "u@h:/remote/"]


def test_argv_delete_and_excludes():
    argv = build_rsync_argv(_cfg(), "/a", "/b", delete=True, excludes=(".git/", "solutions/"))
    assert "--delete" in argv
    assert argv.count("--exclude") == 2
    i = argv.index("--exclude")
    assert argv[i + 1] == ".git/"


def test_argv_no_identity_omits_i():
    cfg = SshConfig(host="h", user="u", port=22, identity_file=None,
                    known_hosts_file="/kh")
    joined = " ".join(build_rsync_argv(cfg, "/a", "/b", delete=False, excludes=()))
    assert "-i " not in joined
