from ray_dispatcher.models import Project, SecretFile
from ray_dispatcher.provisioning import RemoteLayout
from ray_dispatcher.scheduling import secret_env_map


def _project(secrets):
    return Project(path="/p", project_id="dfaas", python="3.10.18",
                   uv_version="0.11.25", secrets=secrets)


def test_maps_env_var_to_remote_secret_path():
    lo = RemoteLayout("/home/ubuntu", "dfaas")
    p = _project((SecretFile(source="~/g.lic", remote_name="g.lic", env_var="GRB_LICENSE_FILE"),))
    assert secret_env_map(p, lo) == {
        "GRB_LICENSE_FILE": "/home/ubuntu/.ray_dispatcher/secrets/dfaas/g.lic"
    }


def test_skips_secrets_without_env_var():
    lo = RemoteLayout("/home/ubuntu", "dfaas")
    p = _project((SecretFile(source="~/a", remote_name="a", env_var=None),))
    assert secret_env_map(p, lo) == {}
