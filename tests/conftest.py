import pytest
import ray


@pytest.fixture(autouse=True)
def _ray_isolation():
    yield
    if ray.is_initialized():
        ray.shutdown()
