"""Content digests for cache invalidation (spec §6.2). Pure; reads local files."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Sequence
from pathlib import Path

from .models import Project


def _excluded(rel: str, excludes: Sequence[str]) -> bool:
    # ponytail: path-prefix excludes (e.g. ".venv/"), not full rsync globs —
    #           sufficient for Project.exclude defaults.
    for raw in excludes:
        e = raw.rstrip("/")
        if rel == e or rel.startswith(e + "/"):
            return True
    return False


def _iter_source_files(root: Path, excludes: Sequence[str]) -> list[str]:
    results: list[str] = []

    def rec(directory: Path, prefix: str) -> None:
        for entry in sorted(os.scandir(directory), key=lambda e: e.name):
            rel = f"{prefix}{entry.name}"
            if _excluded(rel, excludes):
                continue
            if entry.is_symlink():
                results.append(rel)
            elif entry.is_dir():
                rec(Path(entry.path), rel + "/")
            elif entry.is_file():
                results.append(rel)

    rec(root, "")
    return results


def source_digest(root: str, excludes: Sequence[str]) -> str:
    root_path = Path(root)
    h = hashlib.sha256()
    for rel in _iter_source_files(root_path, excludes):
        p = root_path / rel
        h.update(os.fsencode(rel))
        h.update(b"\0")
        if p.is_symlink():
            h.update(b"L")
            h.update(os.fsencode(os.readlink(p)))
        else:
            mode = p.stat().st_mode & 0o777
            h.update(f"M{mode:o}".encode())
            h.update(p.read_bytes())
        h.update(b"\0")
    return h.hexdigest()


def environment_digest(
    project: Project, *, platform: str, sync_flags: Sequence[str]
) -> str:
    base = Path(project.path)
    h = hashlib.sha256()
    for fname in ("pyproject.toml", "uv.lock"):
        h.update((base / fname).read_bytes())
        h.update(b"\0")
    for field in (
        project.python,
        project.uv_version,
        "\0".join(project.dependency_groups),
        "\0".join(sync_flags),
        platform,
    ):
        h.update(field.encode())
        h.update(b"\0")
    return h.hexdigest()


def runner_digest(runner_path: str) -> str:
    return hashlib.sha256(Path(runner_path).read_bytes()).hexdigest()
