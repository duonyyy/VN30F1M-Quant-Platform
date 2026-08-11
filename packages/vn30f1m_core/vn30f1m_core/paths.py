"""Filesystem layout for the standalone VN30F1M platform."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _package_root() -> Path:
    """Return the repository root when imported from the source checkout."""

    # .../vn30f1m_platform/packages/vn30f1m_core/vn30f1m_core/paths.py
    return Path(__file__).resolve().parents[3]


@dataclass(frozen=True, slots=True)
class ProjectPaths:
    """Canonical paths used by all platform modules.

    The class only describes paths. It does not create directories as a side
    effect, which keeps commands such as ``status`` safe and deterministic.
    """

    root: Path

    @classmethod
    def from_root(cls, root: str | os.PathLike[str] | None = None) -> "ProjectPaths":
        configured_root = root or os.getenv("VN30F1M_PROJECT_ROOT")
        resolved_root = Path(configured_root).expanduser() if configured_root else _package_root()
        return cls(resolved_root.resolve())

    @property
    def apps(self) -> Path:
        return self.root / "apps"

    @property
    def packages(self) -> Path:
        return self.root / "packages"

    @property
    def pipelines(self) -> Path:
        return self.root / "pipelines"

    @property
    def infra(self) -> Path:
        return self.root / "infra"

    @property
    def docs(self) -> Path:
        return self.root / "docs"

    @property
    def legacy(self) -> Path:
        return self.root / "legacy"

    @property
    def lakehouse(self) -> Path:
        return self.root / "lakehouse"

    @property
    def landing(self) -> Path:
        return self.lakehouse / "landing"

    @property
    def bronze(self) -> Path:
        return self.lakehouse / "bronze"

    @property
    def silver(self) -> Path:
        return self.lakehouse / "silver"

    @property
    def gold(self) -> Path:
        return self.lakehouse / "gold"

    @property
    def reports(self) -> Path:
        return self.lakehouse / "reports"

    @property
    def artifacts(self) -> Path:
        return self.root / "artifacts"

    @property
    def logs(self) -> Path:
        return self.root / "logs"

    def managed_paths(self) -> dict[str, Path]:
        """Return the paths relevant to status checks and orchestration."""

        return {
            "root": self.root,
            "apps": self.apps,
            "packages": self.packages,
            "pipelines": self.pipelines,
            "infra": self.infra,
            "docs": self.docs,
            "legacy": self.legacy,
            "lakehouse": self.lakehouse,
            "landing": self.landing,
            "bronze": self.bronze,
            "silver": self.silver,
            "gold": self.gold,
            "reports": self.reports,
            "artifacts": self.artifacts,
            "logs": self.logs,
        }

    def existence(self) -> dict[str, bool]:
        """Return whether each managed path currently exists."""

        return {name: path.exists() for name, path in self.managed_paths().items()}

    def ensure_runtime_dirs(self) -> None:
        """Create generated directories when a pipeline explicitly requests it."""

        for path in (self.landing, self.bronze, self.silver, self.gold, self.reports, self.artifacts, self.logs):
            path.mkdir(parents=True, exist_ok=True)
