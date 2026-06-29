import json

from ray_dispatcher.models import FailureKind, Job, JobStatus, RetryPolicy
from ray_dispatcher.provisioning import RemoteLayout
from ray_dispatcher.results import JobLayout
from ray_dispatcher.scheduling import HostRuntime, Lease, run_job
from ray_dispatcher.ssh import CommandResult, FakeTransport, TransportError


class FakeLease:
    """Hands out hosts in order; records the exclude set seen on each acquire."""

    def __init__(self, hosts):
        self._hosts = list(hosts)
        self._n = 0
        self.excludes = []

    def acquire(self, attempt_id, *, exclude=()):
        self.excludes.append(set(exclude))
        host = self._hosts[self._n]
        self._n += 1
        return Lease(token=f"tok{self._n}", host=host, slot=0,
                     attempt_id=attempt_id, expiry_s=0.0, heartbeat_s=0.0)

    def release(self, token):
        pass


def _runtime(host):
    return HostRuntime(host=host, layout=RemoteLayout("/home/u", "dfaas"),
                       environment_digest="env1", runner_digest="run1",
                       project_path="/proj", secret_env={})


def _ok_transport(rc=0):
    def results(argv):
        if argv[0] == "cat" and argv[1].endswith("result.json"):
            return CommandResult(0, json.dumps(
                {"returncode": rc, "started_at": 1.0, "ended_at": 2.0, "duration_s": 1.5}), "", 0.0)
        return CommandResult(0, "", "", 0.0)
    return FakeTransport(run_results=results)


def _ssh_failing_transport():
    def results(argv):
        if argv[0] == "python3":             # the runner invocation
            raise TransportError("ssh dropped")
        return CommandResult(0, "", "", 0.0)
    return FakeTransport(run_results=results)


def _layout(tmp_path, job_id="jobA"):
    return JobLayout(str(tmp_path / "results"), "b1", job_id)


def test_success_first_attempt(tmp_path):
    lease = FakeLease(["a"])
    job = Job(id="jobA", command=("python", "run.py"))  # no outputs
    r = run_job(job, batch_id="b1", lease=lease,
                runtime_for=_runtime, transport_for=lambda h: _ok_transport(0),
                local=_layout(tmp_path), policy=RetryPolicy())
    assert r.status is JobStatus.SUCCEEDED
    assert r.host == "a"
    assert len(r.attempts) == 1
    assert lease.excludes == [set()]        # first acquire excludes nothing


def test_ssh_failure_retries_on_different_host(tmp_path):
    lease = FakeLease(["a", "b"])
    transports = {"a": _ssh_failing_transport(), "b": _ok_transport(0)}
    job = Job(id="jobA", command=("python", "run.py"))
    r = run_job(job, batch_id="b1", lease=lease,
                runtime_for=_runtime, transport_for=lambda h: transports[h],
                local=_layout(tmp_path), policy=RetryPolicy())
    assert r.status is JobStatus.SUCCEEDED
    assert r.host == "b"                     # retried elsewhere
    assert len(r.attempts) == 2
    assert r.attempts[0].failure_kind is FailureKind.SSH
    assert lease.excludes == [set(), {"a"}]  # second acquire excludes the tried host


def test_ssh_failure_exhausts_attempts(tmp_path):
    lease = FakeLease(["a", "b"])
    job = Job(id="jobA", command=("python", "run.py"))
    r = run_job(job, batch_id="b1", lease=lease,
                runtime_for=_runtime, transport_for=lambda h: _ssh_failing_transport(),
                local=_layout(tmp_path), policy=RetryPolicy())  # max_attempts=2
    assert r.status is JobStatus.FAILED
    assert r.attempts[-1].failure_kind is FailureKind.SSH
    assert len(r.attempts) == 2              # stopped at the budget


def test_command_failure_is_not_retried(tmp_path):
    lease = FakeLease(["a", "b"])
    job = Job(id="jobA", command=("python", "run.py"))
    r = run_job(job, batch_id="b1", lease=lease,
                runtime_for=_runtime, transport_for=lambda h: _ok_transport(rc=3),
                local=_layout(tmp_path), policy=RetryPolicy())
    assert r.status is JobStatus.FAILED
    assert r.returncode == 3
    assert r.attempts[-1].failure_kind is FailureKind.COMMAND
    assert len(r.attempts) == 1              # COMMAND not retried
