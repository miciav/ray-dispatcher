import threading

from ray_dispatcher.models import FailureKind, Job, JobStatus
from ray_dispatcher.provisioning import RemoteLayout
from ray_dispatcher.results import JobLayout
from ray_dispatcher.scheduling import HostRuntime, execute_attempt
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _runtime(host: str = "10.0.0.1") -> HostRuntime:
    layout = RemoteLayout("/home/ubuntu", "dfaas")
    return HostRuntime(
        host=host,
        layout=layout,
        environment_digest="env123",
        runner_digest="run123",
        project_path="/proj",
        secret_env={},
    )


def test_execute_attempt_returns_timeout_when_runner_exceeds_timeout_s(tmp_path):
    """Timeout path: runner blocks past timeout_s → TIMED_OUT AttemptResult (§8.1)."""
    runner_started = threading.Event()
    runner_done = threading.Event()

    def results(argv: list[str]) -> CommandResult:
        cmd = " ".join(argv)
        if "python3" in cmd:  # runner invocation — block until released
            runner_started.set()
            runner_done.wait(timeout=5.0)
            return CommandResult(0, "", "", 0.0)
        if argv[0] == "cat" and "pid" in cmd:  # PID file read
            return CommandResult(0, '{"pid": 111, "pgid": 222}', "", 0.0)
        if "kill" in cmd and "-0" in cmd:  # terminate_process_group probe
            runner_done.set()
            return CommandResult(1, "", "", 0.0)  # pgid already gone → terminates immediately
        return CommandResult(0, "", "", 0.0)

    transport = FakeTransport(run_results=results)
    layout = JobLayout(str(tmp_path), "b1", "j1")
    job = Job(id="j1", command=("sleep", "99"), timeout_s=0.05)  # very short timeout

    result_holder: list = []

    def run():
        result_holder.append(
            execute_attempt(transport, _runtime(), job, batch_id="b1", attempt=1, local=layout)
        )

    t = threading.Thread(target=run)
    t.start()
    runner_started.wait(timeout=5.0)  # wait for runner to start
    t.join(timeout=10.0)

    assert len(result_holder) == 1
    r = result_holder[0]
    assert r.status == JobStatus.TIMED_OUT
    assert r.failure_kind == FailureKind.TIMEOUT
    assert r.returncode is None


def test_execute_attempt_no_timeout_when_timeout_s_is_none(tmp_path):
    """When timeout_s is None, the runner is invoked without threading (§8.1 baseline)."""
    def results(argv: list[str]) -> CommandResult:
        if argv[0] == "cat":
            return CommandResult(0, '{"returncode": 0, "duration_s": 0.0}', "", 0.0)
        return CommandResult(0, "", "", 0.0)

    transport = FakeTransport(run_results=results)
    layout = JobLayout(str(tmp_path), "b1", "j2")
    job = Job(id="j2", command=("echo", "hi"))  # timeout_s=None by default

    result = execute_attempt(transport, _runtime(), job, batch_id="b1", attempt=1, local=layout)
    assert result.status == JobStatus.SUCCEEDED
