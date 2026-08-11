"""Historical OHLCV loader and resampling utilities.

The loader is deliberately independent from Spark and Kafka. It produces the
canonical ``ohlcv_intraday`` contract that later pipeline stages consume.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CANONICAL_COLUMNS: Final[tuple[str, ...]] = (
    "symbol",
    "event_time",
    "trading_date",
    "timeframe",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
    "source_record_id",
    "ingest_run_id",
    "ingested_at",
    "quality_status",
)
REQUIRED_SOURCE_COLUMNS: Final[tuple[str, ...]] = ("event_time", "open", "high", "low", "close", "volume")
ALLOWED_TIMEFRAMES: Final[frozenset[str]] = frozenset({"1m", "5m", "15m", "30m"})
RESAMPLE_RULES: Final[dict[str, str]] = {"5m": "5min", "15m": "15min", "30m": "30min"}
LOCAL_MARKET_TIMEZONE: Final[str] = "Asia/Ho_Chi_Minh"


class DataContractError(ValueError):
    """Raised when input cannot satisfy the canonical OHLCV contract."""


@dataclass(frozen=True, slots=True)
class LoadSummary:
    """Small, serializable summary of a loader run."""

    input_path: str
    source_rows: int
    output_rows: int
    source_timeframe: str
    output_timeframe: str
    min_event_time: str
    max_event_time: str
    output_path: str | None = None
    ingest_run_id: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "input_path": self.input_path,
            "source_rows": self.source_rows,
            "output_rows": self.output_rows,
            "source_timeframe": self.source_timeframe,
            "output_timeframe": self.output_timeframe,
            "min_event_time": self.min_event_time,
            "max_event_time": self.max_event_time,
            "output_path": self.output_path,
            "ingest_run_id": self.ingest_run_id,
        }


def make_ingest_run_id(path: Path) -> str:
    """Create a deterministic run id from the source file content."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"csv_{digest.hexdigest()[:16]}"


def _parse_event_time(values: pd.Series, source_timezone: str) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce")
    if parsed.isna().any():
        bad_rows = parsed[parsed.isna()].index[:5].tolist()
        raise DataContractError(f"Invalid event_time at rows {bad_rows}")

    timezone = getattr(parsed.dt, "tz", None)
    if timezone is None:
        parsed = parsed.dt.tz_localize(source_timezone, ambiguous="raise", nonexistent="raise")
    else:
        parsed = parsed.dt.tz_convert("UTC")
    return parsed.dt.tz_convert("UTC")


def _resolve_column(columns: list[str], *aliases: str) -> str:
    normalized = {column.strip().lower(): column for column in columns}
    for alias in aliases:
        if alias.lower() in normalized:
            return normalized[alias.lower()]
    raise DataContractError(f"Missing required column; expected one of {aliases}")


def _canonicalize_source_columns(frame: pd.DataFrame) -> pd.DataFrame:
    columns = [str(column) for column in frame.columns]
    mapping = {
        "event_time": _resolve_column(columns, "event_time", "datetime", "date", "timestamp"),
        "open": _resolve_column(columns, "open"),
        "high": _resolve_column(columns, "high"),
        "low": _resolve_column(columns, "low"),
        "close": _resolve_column(columns, "close"),
        "volume": _resolve_column(columns, "volume"),
    }
    return frame.rename(columns={source: target for target, source in mapping.items()})[
        list(mapping.keys())
    ].copy()


def _normalize_frame(
    frame: pd.DataFrame,
    *,
    symbol: str,
    source: str,
    source_timezone: str,
    ingest_run_id: str,
    source_record_offset: int = 0,
) -> pd.DataFrame:
    normalized = frame.copy()
    normalized["event_time"] = _parse_event_time(normalized["event_time"], source_timezone)
    for column in ("open", "high", "low", "close", "volume"):
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized["symbol"] = symbol
    normalized["trading_date"] = normalized["event_time"].dt.tz_convert(LOCAL_MARKET_TIMEZONE).dt.date
    normalized["timeframe"] = "1m"
    normalized["source"] = source
    normalized["source_record_id"] = pd.Series(
        [str(source_record_offset + index) for index in range(len(normalized))],
        index=normalized.index,
        dtype="string",
    )
    normalized["ingest_run_id"] = ingest_run_id
    normalized["ingested_at"] = pd.Timestamp.now(tz="UTC")
    normalized["quality_status"] = "valid"
    normalized = normalized[list(CANONICAL_COLUMNS)]
    validate_ohlcv(normalized)
    return normalized.sort_values(["symbol", "event_time"]).reset_index(drop=True)


def validate_ohlcv(frame: pd.DataFrame) -> None:
    """Validate an OHLCV frame against the Phase 01 contract."""

    missing = [column for column in CANONICAL_COLUMNS if column not in frame.columns]
    if missing:
        raise DataContractError(f"Missing canonical columns: {missing}")
    if frame.empty:
        raise DataContractError("OHLCV input is empty")

    key = ["symbol", "event_time", "timeframe"]
    if frame.duplicated(key).any():
        raise DataContractError("Duplicate business key in OHLCV input")
    if frame["event_time"].isna().any() or frame["event_time"].dt.tz is None:
        raise DataContractError("event_time must be timezone-aware")
    if not frame["timeframe"].isin(ALLOWED_TIMEFRAMES).all():
        raise DataContractError("Unsupported timeframe in OHLCV input")

    numeric = frame[["open", "high", "low", "close", "volume"]]
    if numeric.isna().any().any() or (~numeric.apply(lambda column: column.map(pd.api.types.is_number)).all()).any():
        raise DataContractError("OHLCV numeric columns contain null or non-numeric values")
    if not np.isfinite(numeric.to_numpy(dtype=float)).all():
        raise DataContractError("OHLCV numeric columns contain non-finite values")
    if (frame[["open", "high", "low", "close"]] <= 0).any().any():
        raise DataContractError("OHLC prices must be greater than zero")
    if (frame["volume"] < 0).any():
        raise DataContractError("Volume must be non-negative")
    if (frame["high"] < frame[["open", "close", "low"]].max(axis=1)).any():
        raise DataContractError("High is lower than one of open/close/low")
    if (frame["low"] > frame[["open", "close", "high"]].min(axis=1)).any():
        raise DataContractError("Low is higher than one of open/close/high")


def load_historical_csv(
    path: str | Path,
    *,
    symbol: str = "VN30F1M",
    source_timezone: str = LOCAL_MARKET_TIMEZONE,
    ingest_run_id: str | None = None,
) -> pd.DataFrame:
    """Read and normalize the legacy historical CSV into canonical 1m rows."""

    input_path = Path(path).expanduser().resolve()
    if not input_path.is_file():
        raise FileNotFoundError(f"Historical CSV not found: {input_path}")
    run_id = ingest_run_id or make_ingest_run_id(input_path)
    source_frame = pd.read_csv(input_path)
    canonical_source = _canonicalize_source_columns(source_frame)
    return _normalize_frame(
        canonical_source,
        symbol=symbol,
        source="historical_csv",
        source_timezone=source_timezone,
        ingest_run_id=run_id,
    )


def resample_ohlcv(frame: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """Resample canonical OHLCV without filling market-session gaps."""

    if timeframe not in ALLOWED_TIMEFRAMES:
        raise DataContractError(f"Unsupported target timeframe: {timeframe}")
    validate_ohlcv(frame)
    if timeframe == "1m":
        return frame.copy()

    groups: list[pd.DataFrame] = []
    for (symbol, trading_date), group in frame.groupby(["symbol", "trading_date"], sort=True):
        indexed = group.sort_values("event_time").set_index("event_time")
        bars = indexed[["open", "high", "low", "close", "volume"]].resample(
            RESAMPLE_RULES[timeframe], label="left", closed="left"
        ).agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        bars = bars.dropna(subset=["open", "high", "low", "close", "volume"])
        if bars.empty:
            continue
        bars = bars.reset_index()
        bars["symbol"] = symbol
        bars["trading_date"] = trading_date
        bars["timeframe"] = timeframe
        bars["source"] = group["source"].iloc[0]
        bars["source_record_id"] = pd.Series([pd.NA] * len(bars), dtype="string")
        bars["ingest_run_id"] = group["ingest_run_id"].iloc[0]
        bars["ingested_at"] = group["ingested_at"].max()
        bars["quality_status"] = "valid"
        groups.append(bars[list(CANONICAL_COLUMNS)])

    if not groups:
        raise DataContractError(f"Resampling produced no {timeframe} bars")
    result = pd.concat(groups, ignore_index=True).sort_values(["symbol", "event_time"])
    result = result.reset_index(drop=True)
    validate_ohlcv(result)
    return result


def write_ohlcv_parquet(frame: pd.DataFrame, output_root: str | Path) -> Path:
    """Write a canonical frame as an immutable, partitioned Parquet dataset."""

    validate_ohlcv(frame)
    root = Path(output_root).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    run_id = str(frame["ingest_run_id"].iloc[0])
    safe_run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)
    table = pa.Table.from_pandas(frame, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=root,
        partition_cols=["symbol", "timeframe", "trading_date"],
        basename_template=f"part-{safe_run_id}-{{i}}.parquet",
        existing_data_behavior="overwrite_or_ignore",
        max_partitions=4096,
    )
    return root
