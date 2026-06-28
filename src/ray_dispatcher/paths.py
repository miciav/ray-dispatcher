"""Path normalization and containment checks (spec §4.3, §6, §7)."""

from __future__ import annotations

import posixpath
from pathlib import Path

from .errors import PathValidationError


def normalize_relative(path: str, *, field: str = "path") -> str:
    """Return a normalized, run-root-relative POSIX path or raise.

    Rejects empty, NUL-containing, absolute, and any path with a ``..``
    component. Does not touch the filesystem.
    """
    if not isinstance(path, str) or path == "":
        raise PathValidationError(f"{field} must be a non-empty string")
    if "\x00" in path:
        raise PathValidationError(f"{field} must not contain NUL bytes")
    if path.startswith("/"):
        raise PathValidationError(f"{field} must be relative, got absolute: {path!r}")
    if ".." in path.split("/"):
        raise PathValidationError(f"{field} must not contain '..': {path!r}")
    normalized = posixpath.normpath(path)
    if normalized.startswith("/") or normalized == ".." or normalized.startswith("../"):
        raise PathValidationError(f"{field} escapes the root: {path!r}")
    return normalized


def ensure_within(root: Path, relative: str, *, field: str = "path") -> Path:
    """Resolve ``relative`` beneath ``root`` (following symlinks) and verify
    the resolved path does not escape ``root``. Raises on escape."""
    rel = normalize_relative(relative, field=field)
    root_resolved = root.resolve()
    candidate = (root_resolved / rel).resolve()
    if candidate != root_resolved and root_resolved not in candidate.parents:
        raise PathValidationError(f"{field} escapes root {root}: {relative!r}")
    return candidate
