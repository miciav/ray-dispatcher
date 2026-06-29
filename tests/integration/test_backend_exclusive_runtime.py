import ray

from ray_dispatcher.backends.ssh_ray import SshRayBackend
from ray_dispatcher.errors import RayRuntimeConflictError
from ray_dispatcher.models import Inventory, Project, RemoteHost


def _project():
    return Project(path="/proj", project_id="dfaas", python="3.10.18", uv_version="0.11.25")


def test_setup_rejects_preinitialized_ray_without_shutting_it_down():
    ray.init(address="local", namespace="preexisting", num_cpus=1)
    try:
        backend = SshRayBackend()
        inv = Inventory((RemoteHost("10.0.0.1", user="ubuntu"),))
        try:
            backend.setup(inv, _project())
            raise AssertionError("expected RayRuntimeConflictError")
        except RayRuntimeConflictError:
            pass
        assert ray.is_initialized()  # the caller's runtime must NOT be shut down
    finally:
        ray.shutdown()
