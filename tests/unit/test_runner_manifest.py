from ray_dispatcher.models import Job
from ray_dispatcher.provisioning import RunPaths
from ray_dispatcher.scheduling import build_runner_manifest


def test_manifest_keys_match_remote_runner():
    run = RunPaths("/home/u/.ray_dispatcher/runs/b/jobA/1")
    job = Job(id="jobA", command=("python", "run.py", "--n", "5"),
              env={"FOO": "bar"}, cwd="sub/dir")
    m = build_runner_manifest(
        job, run_root=run.run_root, venv="/env/.venv", run=run,
        secret_env={"GRB_LICENSE_FILE": "/secrets/g.lic"},
    )
    assert m["argv"] == ["python", "run.py", "--n", "5"]
    assert m["cwd"] == "/home/u/.ray_dispatcher/runs/b/jobA/1/run/sub/dir"
    assert m["env"] == {"FOO": "bar"}
    assert m["secret_env"] == {"GRB_LICENSE_FILE": "/secrets/g.lic"}
    assert m["venv_bin"] == "/env/.venv/bin"
    assert m["virtual_env"] == "/env/.venv"
    assert m["stdout_path"] == run.stdout
    assert m["stderr_path"] == run.stderr
    assert m["pid_path"] == run.pid
    assert m["result_path"] == run.result
    # exactly the keys remote_runner.py reads — no more, no less
    assert set(m) == {"argv", "cwd", "env", "secret_env", "venv_bin",
                      "virtual_env", "stdout_path", "stderr_path", "pid_path", "result_path"}


def test_manifest_cwd_dot_is_run_root():
    run = RunPaths("/r/1")
    job = Job(id="j", command=("echo", "hi"))  # cwd defaults to "."
    m = build_runner_manifest(job, run_root=run.run_root, venv="/v", run=run, secret_env={})
    assert m["cwd"] == "/r/1/run"
