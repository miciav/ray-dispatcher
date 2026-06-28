import json
import subprocess
import sys
from pathlib import Path

RUNNER = str(Path("src/ray_dispatcher/remote_runner.py").resolve())


def _manifest(tmp_path, argv, env=None, secret_env=None):
    m = {
        "argv": argv,
        "cwd": str(tmp_path),
        "env": env or {},
        "secret_env": secret_env or {},
        "venv_bin": str(tmp_path / "venv" / "bin"),
        "virtual_env": str(tmp_path / "venv"),
        "stdout_path": str(tmp_path / "out.log"),
        "stderr_path": str(tmp_path / "err.log"),
        "pid_path": str(tmp_path / "pid.json"),
        "result_path": str(tmp_path / "result.json"),
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(m))
    return m, str(path)


def _invoke(manifest_path):
    return subprocess.run([sys.executable, RUNNER, manifest_path],
                          capture_output=True, text=True)


def test_runner_captures_output_and_returncode(tmp_path):
    m, path = _manifest(
        tmp_path,
        [sys.executable, "-c", "import sys; sys.stdout.write('hi'); "
                              "sys.stderr.write('warn'); sys.exit(3)"],
    )
    proc = _invoke(path)
    assert proc.returncode == 0  # runner managed the child successfully
    assert Path(m["stdout_path"]).read_bytes() == b"hi"
    assert Path(m["stderr_path"]).read_bytes() == b"warn"
    result = json.loads(Path(m["result_path"]).read_text())
    assert result["returncode"] == 3
    assert result["duration_s"] >= 0
    pid = json.loads(Path(m["pid_path"]).read_text())
    assert isinstance(pid["pid"], int) and isinstance(pid["pgid"], int)


def test_runner_applies_venv_and_secret_env(tmp_path):
    m, path = _manifest(
        tmp_path,
        [sys.executable, "-c",
         "import os; open(os.environ['OUT'],'w').write("
         "os.environ['PATH'].split(os.pathsep)[0] + '|' + "
         "os.environ['VIRTUAL_ENV'] + '|' + os.environ['LIC'])"],
        env={"OUT": str(tmp_path / "probe.txt")},
        secret_env={"LIC": "/remote/gurobi.lic"},
    )
    _invoke(path)
    first_path, venv, lic = (tmp_path / "probe.txt").read_text().split("|")
    assert first_path == m["venv_bin"]
    assert venv == m["virtual_env"]
    assert lic == "/remote/gurobi.lic"
