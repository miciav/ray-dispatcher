import os
from pathlib import Path

from ray_dispatcher.digests import source_digest


def _tree(root: Path):
    (root / "pkg").mkdir()
    (root / "pkg" / "a.py").write_text("print('a')")
    (root / "run.py").write_text("print('run')")
    (root / ".venv").mkdir()
    (root / ".venv" / "junk").write_text("junk")


def test_source_digest_is_stable(tmp_path):
    _tree(tmp_path)
    d1 = source_digest(str(tmp_path), excludes=(".venv/",))
    d2 = source_digest(str(tmp_path), excludes=(".venv/",))
    assert d1 == d2
    assert len(d1) == 64  # sha256 hex


def test_excludes_are_applied(tmp_path):
    _tree(tmp_path)
    with_venv = source_digest(str(tmp_path), excludes=())
    without_venv = source_digest(str(tmp_path), excludes=(".venv/",))
    assert with_venv != without_venv


def test_content_change_changes_digest(tmp_path):
    _tree(tmp_path)
    before = source_digest(str(tmp_path), excludes=(".venv/",))
    (tmp_path / "run.py").write_text("print('CHANGED')")
    assert source_digest(str(tmp_path), excludes=(".venv/",)) != before


def test_mode_change_changes_digest(tmp_path):
    _tree(tmp_path)
    before = source_digest(str(tmp_path), excludes=(".venv/",))
    os.chmod(tmp_path / "run.py", 0o755)
    assert source_digest(str(tmp_path), excludes=(".venv/",)) != before


def test_symlink_target_is_recorded_not_followed(tmp_path):
    _tree(tmp_path)
    os.symlink("run.py", tmp_path / "link")
    before = source_digest(str(tmp_path), excludes=(".venv/",))
    os.unlink(tmp_path / "link")
    os.symlink("pkg/a.py", tmp_path / "link")  # same name, different target
    assert source_digest(str(tmp_path), excludes=(".venv/",)) != before
