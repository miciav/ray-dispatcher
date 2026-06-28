from pathlib import Path

import pytest

from ray_dispatcher.digests import environment_digest
from ray_dispatcher.models import Project


def _project(tmp_path: Path, **over) -> Project:
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n")
    (tmp_path / "uv.lock").write_text("# lock v1\n")
    kwargs = dict(path=str(tmp_path), project_id="x", python="3.10.18",
                  uv_version="0.11.25")
    kwargs.update(over)
    return Project(**kwargs)


def test_env_digest_stable(tmp_path):
    p = _project(tmp_path)
    a = environment_digest(p, platform="linux-x86_64", sync_flags=("--locked",))
    b = environment_digest(p, platform="linux-x86_64", sync_flags=("--locked",))
    assert a == b and len(a) == 64


@pytest.mark.parametrize("mutate", [
    lambda tmp, p: ((tmp / "uv.lock").write_text("# lock v2\n"), p)[1],
    lambda tmp, p: ((tmp / "pyproject.toml").write_text("[project]\nname='y'\n"), p)[1],
])
def test_env_digest_changes_on_file_change(tmp_path, mutate):
    p = _project(tmp_path)
    before = environment_digest(p, platform="linux-x86_64", sync_flags=("--locked",))
    p = mutate(tmp_path, p)
    assert environment_digest(p, platform="linux-x86_64", sync_flags=("--locked",)) != before


def test_env_digest_changes_on_metadata(tmp_path):
    p = _project(tmp_path)
    base = environment_digest(p, platform="linux-x86_64", sync_flags=("--locked",))
    p2 = _project(tmp_path, python="3.10.19")
    assert environment_digest(p2, platform="linux-x86_64", sync_flags=("--locked",)) != base
    assert environment_digest(p, platform="linux-aarch64", sync_flags=("--locked",)) != base
    assert environment_digest(p, platform="linux-x86_64", sync_flags=("--frozen",)) != base
    p3 = _project(tmp_path, dependency_groups=("dev",))
    assert environment_digest(p3, platform="linux-x86_64", sync_flags=("--locked",)) != base
    p4 = _project(tmp_path, uv_version="0.12.0")
    assert environment_digest(p4, platform="linux-x86_64", sync_flags=("--locked",)) != base
