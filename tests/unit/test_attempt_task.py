import ray

from ray_dispatcher.backends.ssh_ray import HostLease, _attempt_task
from ray_dispatcher.models import (
    Job,
    JobStatus,
    RemoteHost,
    RetryPolicy,
)
from ray_dispatcher.provisioning import RemoteLayout
from ray_dispatcher.results import JobLayout
from ray_dispatcher.scheduling import HostRuntime
from ray_dispatcher.ssh import CommandResult, FakeTransport


def _canned_runtime(host_name: str) -> HostRuntime:
    layout = RemoteLayout("/home/ubuntu", "dfaas")
    return HostRuntime(
        host=host_name,
        layout=layout,
        environment_digest="env123",
        runner_digest="run123",
        project_path="/proj",
        secret_env={},
    )


def test_attempt_task_has_correct_ray_decoration():
    """Verify the task declares the exact Ray options from §3.2.3 and §11."""
    opts = _attempt_task._default_options  # type: ignore[attr-defined]
    assert opts.get("num_cpus") == 0
    assert opts.get("resources") == {"vm_slot": 1}
    assert opts.get("max_retries") == 0


def test_attempt_task_succeeds_with_fake_transport(tmp_path):
    ray.init(address="local", namespace="test-task-ok", resources={"vm_slot": 2.0})
    try:
        host = RemoteHost("10.0.0.1", user="ubuntu", slots=1)
        actor = HostLease.remote({"10.0.0.1": 1})
        runtimes = {"10.0.0.1": _canned_runtime("10.0.0.1")}
        inv_hosts = {"10.0.0.1": host}
        local = JobLayout(str(tmp_path), "batch1", "job1")
        job = Job(id="job1", command=("echo", "hi"))

        # ponytail: inline closure so cloudpickle serializes it without a module reference
        def _ok_transport(h: RemoteHost) -> FakeTransport:
            def results(argv: list[str]) -> CommandResult:
                if argv[0] == "cat":
                    return CommandResult(0, '{"returncode": 0, "duration_s": 0.1}', "", 0.0)
                return CommandResult(0, "", "", 0.0)
            return FakeTransport(run_results=results)

        ref = _attempt_task.remote(  # type: ignore[attr-defined]
            job, "batch1", local, runtimes, inv_hosts, actor, _ok_transport, RetryPolicy(),
            heartbeat_interval_s=1.0,
        )
        result = ray.get(ref)
        assert result.status == JobStatus.SUCCEEDED
        assert result.id == "job1"
        # result.json written to disk
        assert local.result_json.exists()
    finally:
        ray.shutdown()


def test_attempt_task_catch_all_returns_internal_on_unexpected_exception(tmp_path):
    """An exception escaping run_job (e.g. malformed result.json) becomes INTERNAL, not a crash."""
    ray.init(address="local", namespace="test-task-internal", resources={"vm_slot": 2.0})
    try:
        host = RemoteHost("10.0.0.1", user="ubuntu", slots=1)
        actor = HostLease.remote({"10.0.0.1": 1})
        runtimes = {"10.0.0.1": _canned_runtime("10.0.0.1")}
        inv_hosts = {"10.0.0.1": host}
        local = JobLayout(str(tmp_path), "batch1", "job2")
        job = Job(id="job2", command=("echo", "hi"))

        def _bad_transport(h: RemoteHost) -> FakeTransport:
            def results(argv: list[str]) -> CommandResult:
                if argv[0] == "cat":
                    return CommandResult(0, "not-json!", "", 0.0)  # malformed result.json
                return CommandResult(0, "", "", 0.0)
            return FakeTransport(run_results=results)

        ref = _attempt_task.remote(  # type: ignore[attr-defined]
            job, "batch1", local, runtimes, inv_hosts, actor, _bad_transport, RetryPolicy(),
        )
        result = ray.get(ref)
        assert result.status == JobStatus.FAILED
        assert result.error is not None and "INTERNAL" in result.error
    finally:
        ray.shutdown()
