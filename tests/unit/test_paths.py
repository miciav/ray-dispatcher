import os

import pytest

from ray_dispatcher.errors import PathValidationError
from ray_dispatcher.paths import ensure_within, normalize_relative


@pytest.mark.parametrize("good,expected", [
    ("a/b/c", "a/b/c"),
    ("./a/./b", "a/b"),
    (".", "."),
    ("config_files/eval_smoke.json", "config_files/eval_smoke.json"),
])
def test_normalize_relative_accepts(good, expected):
    assert normalize_relative(good) == expected


@pytest.mark.parametrize("bad", [
    "",
    "/etc/passwd",
    "../escape",
    "a/../../b",
    "a/../b",          # any '..' component is rejected, even if it would collapse
    "with\x00nul",
])
def test_normalize_relative_rejects(bad):
    with pytest.raises(PathValidationError):
        normalize_relative(bad)


def test_ensure_within_accepts_nested(tmp_path):
    target = ensure_within(tmp_path, "sub/dir/file.txt")
    assert target == (tmp_path.resolve() / "sub/dir/file.txt")


def test_ensure_within_rejects_symlink_escape(tmp_path):
    outside = tmp_path.parent / "outside"
    outside.mkdir()
    link = tmp_path / "link"
    os.symlink(outside, link)
    with pytest.raises(PathValidationError):
        ensure_within(tmp_path, "link/secret.txt")
