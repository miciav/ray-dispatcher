import pytest

from ray_dispatcher.ssh import CommandResult, FakeTransport, TransportError, write_remote_file


def _runs(t):
    return [c[1] for c in t.calls if c[0] == "run"]


def test_write_remote_file_is_printf_tmp_then_mv():
    t = FakeTransport()  # default rc 0
    write_remote_file(t, "/home/u/.ray_dispatcher/runs/b/j/1/manifest.json", '{"a":1}')
    script = _runs(t)[-1][2]  # argv is ["sh", "-c", script]
    assert "printf %s" in script
    assert "manifest.json.tmp" in script
    assert "mv -f" in script
    # the data is shlex-quoted, not bare-interpolated into the shell
    assert "'{\"a\":1}'" in script


def test_write_remote_file_applies_mode():
    t = FakeTransport()
    write_remote_file(t, "/x/f", "data", mode=0o600)
    script = _runs(t)[-1][2]
    assert "chmod 600" in script


def test_write_remote_file_raises_on_failure():
    def results(argv):
        return CommandResult(1, "", "disk full", 0.0)

    t = FakeTransport(run_results=results)
    with pytest.raises(TransportError, match="disk full"):
        write_remote_file(t, "/x/f", "data")
