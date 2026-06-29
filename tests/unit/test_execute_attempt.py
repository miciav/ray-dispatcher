import json

from ray_dispatcher.models import AttemptResult, FailureKind, InputSpec, Job, JobStatus, OutputSpec
from ray_dispatcher.provisioning import RemoteLayout
from ray_dispatcher.results import JobLayout
from ray_dispatcher.scheduling import HostRuntime, execute_attempt
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _runtime():
    return HostRuntime(
        host="10.0.0.1",
        layout=RemoteLayout("/home/u", "dfaas"),
        environment_digest="env123",
        runner_digest="run123",
        project_path="/local/proj",
        secret_env={"GRB_LICENSE_FILE": "/home/u/.ray_dispatcher/secrets/dfaas/g.lic"},
    )


def _transport(returncode=0):
    # Programmable fake: the runner's result.json reports `returncode`; every
    # other remote command (mkdir/rsync/ln/runner) succeeds.
    def results(argv):
        if argv[0] == "cat" and argv[1].endswith("result.json"):
            doc = json.dumps({"returncode": returncode, "started_at": 1.0,
                              "ended_at": 2.0, "duration_s": 1.5})
            return CommandResult(0, doc, "", 0.0)
        return CommandResult(0, "", "", 0.0)
    return FakeTransport(run_results=results)


def _layout(tmp_path):
    return JobLayout(str(tmp_path / "results"), "b1", "jobA")


def _runs(t):
    return [c[1] for c in t.calls if c[0] == "run"]


def test_success_with_no_outputs_publishes_and_succeeds(tmp_path):
    job = Job(id="jobA", command=("python", "run.py"))  # no declared outputs
    t = _transport(returncode=0)
    res = execute_attempt(t, _runtime(), job, batch_id="b1", attempt=1, local=_layout(tmp_path))
    assert isinstance(res, AttemptResult)
    assert res.status is JobStatus.SUCCEEDED
    assert res.returncode == 0
    assert res.failure_kind is None
    assert res.host == "10.0.0.1"
    assert res.duration_s == 1.5
    # logs recorded locally; outputs published (empty) to the job outputs dir
    lo = _layout(tmp_path)
    assert res.stdout_log == str(lo.stdout_log(1))
    assert lo.outputs_dir.is_dir()                 # published
    assert lo.attempt_json(1).is_file()            # attempt.json written


def test_command_failure_classifies_command_and_does_not_publish(tmp_path):
    job = Job(id="jobA", command=("python", "run.py"))
    t = _transport(returncode=3)
    res = execute_attempt(t, _runtime(), job, batch_id="b1", attempt=1, local=_layout(tmp_path))
    assert res.status is JobStatus.FAILED
    assert res.returncode == 3
    assert res.failure_kind is FailureKind.COMMAND
    assert not _layout(tmp_path).outputs_dir.exists()  # no publish on failure


def test_missing_required_output_classifies_output_missing(tmp_path):
    # FakeTransport.pull is a no-op, so a required output never lands -> OUTPUT_MISSING
    job = Job(id="jobA", command=("python", "run.py"),
              outputs=(OutputSpec(source="solutions/out.json", required=True),))
    t = _transport(returncode=0)
    res = execute_attempt(t, _runtime(), job, batch_id="b1", attempt=1, local=_layout(tmp_path))
    assert res.status is JobStatus.FAILED
    assert res.failure_kind is FailureKind.OUTPUT_MISSING
    assert not _layout(tmp_path).outputs_dir.exists()


def test_remote_protocol_is_no_shell_and_ordered(tmp_path):
    job = Job(id="jobA", command=("python", "run.py", "--cfg", "c.json"),
              inputs=(InputSpec(source="c.json", destination="c.json"),))
    t = _transport(returncode=0)
    execute_attempt(t, _runtime(), job, batch_id="b1", attempt=1, local=_layout(tmp_path))
    runs = _runs(t)
    flat = [tok for argv in runs for tok in argv]
    # the runner is invoked by absolute python3 + runner path + manifest path
    assert any(a[0] == "python3" and a[1].endswith("/bin/run123/remote_runner.py")
               and a[2].endswith("/1/manifest.json") for a in runs)
    # §7 no-shell: job command tokens never appear as their own argv anywhere;
    # they travel only inside the manifest JSON (written via printf %s).
    assert "run.py" not in flat
    assert "--cfg" not in flat
    # the run dir leaf is created without -p (existing dir is an error, §7.2)
    assert any("mkdir " in a[2] and "-p" not in a[2].split("&&")[-1] for a in runs
               if a[0] == "sh")
    # the input was pushed (host->VM), not shelled
    pushes = [c[1] for c in t.calls if c[0] == "push"]
    assert any(p[0] == "/local/proj/c.json" and p[1].endswith("/run/c.json") for p in pushes)
