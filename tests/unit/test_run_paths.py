from ray_dispatcher.provisioning import RemoteLayout, RunPaths


def test_run_dir_under_root():
    lo = RemoteLayout("/home/ubuntu", "dfaas")
    assert lo.run_dir("b1", "jobA", 2) == "/home/ubuntu/.ray_dispatcher/runs/b1/jobA/2"


def test_run_paths_layout():
    rp = RunPaths("/home/ubuntu/.ray_dispatcher/runs/b1/jobA/2")
    assert rp.run_root == "/home/ubuntu/.ray_dispatcher/runs/b1/jobA/2/run"
    assert rp.venv == "/home/ubuntu/.ray_dispatcher/runs/b1/jobA/2/run/.venv"
    assert rp.manifest.endswith("/2/manifest.json")
    assert rp.stdout.endswith("/2/stdout.log")
    assert rp.stderr.endswith("/2/stderr.log")
    assert rp.pid.endswith("/2/pid.json")
    assert rp.result.endswith("/2/result.json")


def test_run_paths_from_layout():
    lo = RemoteLayout("/home/ubuntu", "dfaas")
    rp = lo.run_paths("b1", "jobA", 2)
    assert rp.base == lo.run_dir("b1", "jobA", 2)
    assert rp.run_root.endswith("/2/run")
