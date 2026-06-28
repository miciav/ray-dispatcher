import json
import subprocess
import sys
from pathlib import Path

RUNNER = str(Path("src/ray_dispatcher/remote_runner.py").resolve())


def _manifest(tmp_path, argv):
    m = {
        "argv": argv, "cwd": str(tmp_path), "env": {}, "secret_env": {},
        "venv_bin": str(tmp_path / "v" / "bin"), "virtual_env": str(tmp_path / "v"),
        "stdout_path": str(tmp_path / "out.log"), "stderr_path": str(tmp_path / "err.log"),
        "pid_path": str(tmp_path / "pid.json"), "result_path": str(tmp_path / "result.json"),
    }
    p = tmp_path / "m.json"
    p.write_text(json.dumps(m))
    return m, str(p)


def test_child_output_is_forwarded_live_to_runner_streams(tmp_path):
    m, path = _manifest(tmp_path, [sys.executable, "-c",
                                   "import sys; print('forward-me'); "
                                   "print('to-stderr', file=sys.stderr)"])
    proc = subprocess.run([sys.executable, RUNNER, path], capture_output=True, text=True)
    assert "forward-me" in proc.stdout
    assert "to-stderr" in proc.stderr


def test_binary_output_preserved_in_file_and_forwarded_raw(tmp_path):
    m, path = _manifest(tmp_path, [sys.executable, "-c",
                                   "import sys; sys.stdout.buffer.write(b'\\xff\\xfe')"])
    proc = subprocess.run([sys.executable, RUNNER, path], capture_output=True)
    # raw bytes preserved on the VM-side file AND forwarded byte-for-byte
    assert Path(m["stdout_path"]).read_bytes() == b"\xff\xfe"
    assert b"\xff\xfe" in proc.stdout
