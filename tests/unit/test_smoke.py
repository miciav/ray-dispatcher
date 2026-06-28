import ray_dispatcher


def test_package_exposes_version():
    assert isinstance(ray_dispatcher.__version__, str)
    assert ray_dispatcher.__version__
