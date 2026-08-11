"""Environment-backed settings for the VN30F1M platform."""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Mapping

from .paths import ProjectPaths

ALLOWED_TIMEFRAMES = frozenset({"1m", "5m", "15m", "30m"})


def _read_dotenv(path: Path) -> dict[str, str]:
    """Read a small dotenv file without adding a runtime dependency."""

    if not path.is_file():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        values[key] = value
    return values


def _value(name: str, file_values: Mapping[str, str], default: str) -> str:
    return os.environ.get(name, file_values.get(name, default))


def _bool_value(name: str, file_values: Mapping[str, str], default: bool) -> bool:
    raw = _value(name, file_values, str(default)).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean, got {raw!r}")


@dataclass(frozen=True, slots=True)
class Settings:
    """Validated application settings with safe local defaults."""

    project_root: Path
    environment: str
    project_name: str
    default_symbol: str
    timezone: str
    source_timeframe: str
    canonical_timeframe: str
    storage_backend: str
    kafka_enabled: bool
    kafka_bootstrap_servers: str
    kafka_raw_topic: str
    kafka_consumer_group: str
    clickhouse_enabled: bool
    clickhouse_url: str
    dnse_api_key: str = field(default="", repr=False)
    dnse_api_secret: str = field(default="", repr=False)
    dnse_api_token: str = field(default="", repr=False)
    dnse_openapi_url: str = "https://openapi.dnse.com.vn"
    dnse_public_url: str = "https://api.dnse.com.vn"
    dnse_api_version: str = "2026-05-07"
    dnse_timeout_seconds: float = 30.0
    dnse_max_retries: int = 3
    dnse_backoff_factor: float = 0.5

    def __post_init__(self) -> None:
        if self.source_timeframe not in ALLOWED_TIMEFRAMES:
            raise ValueError(f"Unsupported source_timeframe: {self.source_timeframe}")
        if self.canonical_timeframe not in ALLOWED_TIMEFRAMES:
            raise ValueError(f"Unsupported canonical_timeframe: {self.canonical_timeframe}")
        if not self.default_symbol.strip():
            raise ValueError("default_symbol must not be empty")
        if self.storage_backend not in {"local", "minio"}:
            raise ValueError("storage_backend must be 'local' or 'minio'")
        if self.kafka_enabled and not self.kafka_bootstrap_servers.strip():
            raise ValueError("kafka_bootstrap_servers is required when Kafka is enabled")
        if self.dnse_timeout_seconds <= 0:
            raise ValueError("dnse_timeout_seconds must be greater than zero")
        if self.dnse_max_retries < 0:
            raise ValueError("dnse_max_retries must not be negative")
        if self.dnse_backoff_factor < 0:
            raise ValueError("dnse_backoff_factor must not be negative")

    @classmethod
    def from_env(cls, project_root: str | os.PathLike[str] | None = None) -> "Settings":
        paths = ProjectPaths.from_root(project_root)
        file_values = _read_dotenv(paths.root / ".env")
        return cls(
            project_root=paths.root,
            environment=_value("VN30F1M_ENV", file_values, "local"),
            project_name=_value("VN30F1M_PROJECT_NAME", file_values, "VN30F1M Quant Platform"),
            default_symbol=_value("VN30F1M_DEFAULT_SYMBOL", file_values, "VN30F1M"),
            timezone=_value("VN30F1M_TIMEZONE", file_values, "Asia/Ho_Chi_Minh"),
            source_timeframe=_value("VN30F1M_SOURCE_TIMEFRAME", file_values, "1m"),
            canonical_timeframe=_value("VN30F1M_CANONICAL_TIMEFRAME", file_values, "15m"),
            storage_backend=_value("VN30F1M_STORAGE_BACKEND", file_values, "local"),
            kafka_enabled=_bool_value("VN30F1M_KAFKA_ENABLED", file_values, True),
            kafka_bootstrap_servers=_value(
                "VN30F1M_KAFKA_BOOTSTRAP_SERVERS", file_values, "localhost:9092"
            ),
            kafka_raw_topic=_value("VN30F1M_KAFKA_RAW_TOPIC", file_values, "vn30f1m.ohlcv.raw"),
            kafka_consumer_group=_value(
                "VN30F1M_KAFKA_CONSUMER_GROUP", file_values, "vn30f1m-batch-consumer"
            ),
            clickhouse_enabled=_bool_value("VN30F1M_CLICKHOUSE_ENABLED", file_values, False),
            clickhouse_url=_value("VN30F1M_CLICKHOUSE_URL", file_values, "http://localhost:8123"),
            dnse_api_key=_value("DNSE_API_KEY", file_values, ""),
            dnse_api_secret=_value("DNSE_API_SECRET", file_values, ""),
            dnse_api_token=_value("DNSE_API_TOKEN", file_values, ""),
            dnse_openapi_url=_value(
                "DNSE_OPENAPI_URL", file_values, "https://openapi.dnse.com.vn"
            ),
            dnse_public_url=_value("DNSE_PUBLIC_URL", file_values, "https://api.dnse.com.vn"),
            dnse_api_version=_value("DNSE_API_VERSION", file_values, "2026-05-07"),
            dnse_timeout_seconds=float(_value("DNSE_TIMEOUT_SECONDS", file_values, "30")),
            dnse_max_retries=int(_value("DNSE_MAX_RETRIES", file_values, "3")),
            dnse_backoff_factor=float(_value("DNSE_BACKOFF_FACTOR", file_values, "0.5")),
        )

    @property
    def paths(self) -> ProjectPaths:
        return ProjectPaths.from_root(self.project_root)

    def as_dict(self) -> dict[str, object]:
        values = asdict(self)
        values["project_root"] = str(self.project_root)
        values["dnse_api_key_configured"] = bool(self.dnse_api_key)
        values["dnse_api_secret_configured"] = bool(self.dnse_api_secret)
        values["dnse_api_token_configured"] = bool(self.dnse_api_token)
        for secret_name in ("dnse_api_key", "dnse_api_secret", "dnse_api_token"):
            values.pop(secret_name, None)
        values["paths"] = {name: str(path) for name, path in self.paths.managed_paths().items()}
        return values
