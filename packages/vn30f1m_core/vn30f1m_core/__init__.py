"""Core configuration and CLI for the VN30F1M platform."""

__version__ = "0.1.0"

from .paths import ProjectPaths
from .settings import Settings

__all__ = ["ProjectPaths", "Settings", "__version__"]
