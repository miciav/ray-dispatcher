import pytest

from ray_dispatcher.models import Job
from ray_dispatcher.provisioning import RemoteLayout
from ray_dispatcher.results import JobLayout
from ray_dispatcher.scheduling import HostRuntime, execute_attempt
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _runtime() -> HostRuntime:
    return HostRuntime(
        host="10.0.0.1",
        layout=RemoteLayout("/home/u", "proj"),
        environment_digest="env123",
        runner_digest="run123",
        project_path="/local/proj",
        secret_env={},
    )


def _results_cancels_runner(pgid: int):
    """FakeTransport: raise KeyboardInterrupt on python3 invocation, serve pid file."""

    def results(argv: list[str]) -> CommandResult:
        cmd = " ".join(argv)
        if argv[0] == "python3":
            raise KeyboardInterrupt()
        if argv[0] == "cat" and "pid" in cmd:
            return CommandResult(0, f'{{"pid": 111, "pgid": {pgid}}}', "", 0.0)
        if argv[:2] == ["kill", "-0"]:
            return CommandResult(1, "", "", 0.0)  # already dead → probe done
        return CommandResult(0, "", "", 0.0)

    return results


def test_kills_remote_pgid_when_baseexception_interrupts_runner(tmp_path):
    """BaseException during runner invocation (e.g. Ray's TaskCancelledError):
    terminate_process_group is called before the exception propagates."""
    transport = FakeTransport(run_results=_results_cancels_runner(pgid=222))
    job = Job(id="j1", command=("python", "script.py"))  # timeout_s=None

    with pytest.raises(KeyboardInterrupt):
        execute_attempt(
            transport, _runtime(), job,
            batch_id="b1", attempt=1, local=JobLayout(str(tmp_path), "b1", "j1"),
        )

    sent = [c[1] for c in transport.calls if c[0] == "run"]
    assert any(a == ("kill", "-TERM", "-222") for a in sent), (
        "expected kill -TERM -222 to be sent to terminate the orphaned remote process"
    )


def test_baseexception_propagates_even_when_pid_file_absent(tmp_path):
    """If the pid file doesn't exist yet, _kill_remote_pgid is best-effort:
    the original BaseException still propagates (not swallowed, not replaced)."""

    def results(argv: list[str]) -> CommandResult:
        if argv[0] == "python3":
            raise KeyboardInterrupt()
        if argv[0] == "cat":
            return CommandResult(1, "", "no such file", 0.0)
        return CommandResult(0, "", "", 0.0)

    transport = FakeTransport(run_results=results)
    job = Job(id="j2", command=("python", "script.py"))

    with pytest.raises(KeyboardInterrupt):
        execute_attempt(
            transport, _runtime(), job,
            batch_id="b1", attempt=1, local=JobLayout(str(tmp_path), "b1", "j2"),
        )

    sent = [c[1] for c in transport.calls if c[0] == "run"]
    assert not any(a[0] == "kill" and "-TERM" in a for a in sent)
