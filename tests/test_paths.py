from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from vn30f1m_core.paths import ProjectPaths


def test_paths_are_relative_to_project_root():
    repo_root = Path(__file__).resolve().parents[1]
    paths = ProjectPaths.from_root(repo_root)

    assert paths.root == repo_root.resolve()
    assert paths.landing == repo_root / "lakehouse" / "landing"
    assert paths.gold == repo_root / "lakehouse" / "gold"
    assert paths.existence()["root"] is True
    assert paths.existence()["docs"] is True


def test_ensure_runtime_dirs_is_explicit():
    paths = ProjectPaths.from_root(Path(__file__).resolve().parents[1])

    with patch.object(Path, "mkdir") as mkdir:
        paths.ensure_runtime_dirs()

    assert mkdir.call_count == 7
