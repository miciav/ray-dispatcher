from ray_dispatcher.digests import runner_digest


def test_runner_digest_hashes_contents(tmp_path):
    f = tmp_path / "remote_runner.py"
    f.write_text("print('v1')")
    before = runner_digest(str(f))
    assert len(before) == 64
    f.write_text("print('v2')")
    assert runner_digest(str(f)) != before


def test_runner_digest_matches_real_runner():
    # the bundled runner exists and hashes without error
    d = runner_digest("src/ray_dispatcher/remote_runner.py")
    assert len(d) == 64
