"""Leakage-safe technical features for canonical VN30F1M OHLCV data.

Feature rows are kept at the same bar timestamp, but every predictor is shifted
one bar within its symbol. Therefore a row at ``t`` only contains information
available before the decision at ``t``. Warm-up rows are retained with null
features and are filtered by the later training phase.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Final, Iterable

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

FEATURE_SET_VERSION: Final[str] = "vn30f1m_features_v1"
LOCAL_TIMEZONE: Final[str] = "Asia/Ho_Chi_Minh"
REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "symbol", "event_time", "trading_date", "timeframe",
    "open", "high", "low", "close", "volume",
)
BASE_COLUMNS: Final[frozenset[str]] = frozenset(REQUIRED_COLUMNS)


class FeatureContractError(ValueError):
    """Raised when Silver input cannot produce a valid feature contract."""


@dataclass(frozen=True, slots=True)
class FeatureConfig:
    """Versioned feature configuration used for one Gold build."""

    feature_set_version: str = FEATURE_SET_VERSION
    timezone: str = LOCAL_TIMEZONE
    shift_bars: int = 1

    def __post_init__(self) -> None:
        if not self.feature_set_version.strip():
            raise ValueError("feature_set_version must not be empty")
        if not self.timezone.strip():
            raise ValueError("timezone must not be empty")
        if self.shift_bars < 1:
            raise ValueError("shift_bars must be at least 1")

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _validate_input(frame: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise FeatureContractError(f"Missing Silver columns: {missing}")
    if frame.empty:
        raise FeatureContractError("Silver input is empty")

    out = frame.copy()
    out["event_time"] = pd.to_datetime(out["event_time"], utc=True, errors="coerce")
    if out["event_time"].isna().any():
        raise FeatureContractError("event_time must be valid timezone-aware timestamps")
    expected_trading_date = out["event_time"].dt.tz_convert(LOCAL_TIMEZONE).dt.date.astype(str)
    if out["trading_date"].astype(str).tolist() != expected_trading_date.tolist():
        raise FeatureContractError("trading_date does not match event_time")
    for column in ("open", "high", "low", "close", "volume"):
        out[column] = pd.to_numeric(out[column], errors="coerce")
    if out[["open", "high", "low", "close", "volume"]].isna().any().any():
        raise FeatureContractError("Silver OHLCV contains null numeric values")
    key = ["symbol", "event_time", "timeframe"]
    if out.duplicated(key).any():
        raise FeatureContractError("Duplicate Silver business key")
    if (out[["open", "high", "low", "close"]] <= 0).any().any():
        raise FeatureContractError("Silver OHLC prices must be greater than zero")
    if (out["volume"] < 0).any():
        raise FeatureContractError("Silver volume must not be negative")
    if (out["high"] < out[["open", "close", "low"]].max(axis=1)).any():
        raise FeatureContractError("Silver high violates OHLC bounds")
    if (out["low"] > out[["open", "close", "high"]].min(axis=1)).any():
        raise FeatureContractError("Silver low violates OHLC bounds")
    return out.sort_values(["symbol", "event_time"]).reset_index(drop=True)


def _rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    return 100 - 100 / (1 + gain / (loss + 1e-12))


def _true_range(frame: pd.DataFrame) -> pd.Series:
    previous_close = frame["close"].shift(1)
    return pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - previous_close).abs(),
            (frame["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)


def _atr(frame: pd.DataFrame, period: int) -> pd.Series:
    return _true_range(frame).ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def _adx(frame: pd.DataFrame, period: int) -> pd.Series:
    high_diff = frame["high"].diff()
    low_diff = -frame["low"].diff()
    plus_dm = pd.Series(np.where((high_diff > low_diff) & (high_diff > 0), high_diff, 0.0), index=frame.index)
    minus_dm = pd.Series(np.where((low_diff > high_diff) & (low_diff > 0), low_diff, 0.0), index=frame.index)
    tr = _true_range(frame)
    atr_value = tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / (atr_value + 1e-12)
    minus_di = 100 * minus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / (atr_value + 1e-12)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-12)
    return dx.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def _linear_reg_midline(close: pd.Series, period: int) -> pd.Series:
    x = np.arange(period, dtype=float)
    x_mean = x.mean()
    denominator = ((x - x_mean) ** 2).sum()

    def endpoint(values: np.ndarray) -> float:
        y_mean = values.mean()
        slope = ((x - x_mean) * (values - y_mean)).sum() / denominator
        return slope * (period - 1) + (y_mean - slope * x_mean)

    return close.rolling(period, min_periods=period).apply(endpoint, raw=True)


def _build_symbol_features(frame: pd.DataFrame, config: FeatureConfig) -> tuple[pd.DataFrame, list[str]]:
    out = frame.copy()
    close = out["close"]
    volume = out["volume"]
    generated: list[str] = []

    def add(name: str, values: pd.Series | np.ndarray) -> None:
        out[name] = values
        generated.append(name)

    for window in (1, 2, 3, 5, 8, 13, 21, 34, 55):
        add(f"ret_{window}", close.pct_change(window))
        add(f"mom_points_{window}", close.diff(window))

    for window in (5, 10, 20, 40, 80):
        mean = close.rolling(window, min_periods=window).mean()
        std = close.rolling(window, min_periods=window).std(ddof=0)
        add(f"zclose_{window}", (close - mean) / (std + 1e-12))
        ret_1 = close.pct_change(1)
        add(f"volatility_{window}", ret_1.rolling(window, min_periods=window).std(ddof=0))
        add(f"range_mean_{window}", ((out["high"] - out["low"]) / close).rolling(window, min_periods=window).mean())

    for span in (8, 13, 21, 34, 55):
        ema = close.ewm(span=span, adjust=False, min_periods=span).mean()
        add(f"ema_dist_{span}", (close - ema) / (ema + 1e-12))

    for period in (7, 14, 28):
        add(f"rsi_{period}", _rsi(close, period))
    add("atr_14", _atr(out, 14))
    add("atr_20", _atr(out, 20))
    add("atr_pct_14", out["atr_14"] / (close + 1e-12))
    add("atr_z_80", (out["atr_14"] - out["atr_14"].rolling(80).mean()) / (out["atr_14"].rolling(80).std() + 1e-12))
    for period in (8, 14):
        add(f"adx_{period}", _adx(out, period))

    atr_slow = _atr(out, 60)
    add("atr_fast_slow_ratio", out["atr_20"] / (atr_slow + 1e-12))
    add("vol_regime_low", (out["atr_fast_slow_ratio"] < 1.1).astype(float))

    for period in (10, 14, 20, 30, 40):
        midline = _linear_reg_midline(close, period)
        z = (close - midline) / (1.1 * out["atr_20"] + 1e-12)
        add(f"lr_atr_z_{period}", z)
        add(f"lr_atr_z_abs_{period}", z.abs())
        add(f"lr_atr_z_mom1_{period}", z.diff(1))
        add(f"lr_atr_z_mom3_{period}", z.diff(3))
        add(f"lr_atr_cross_up_{period}", ((z.shift(1) < 0.7) & (z >= 0.7)).astype(float))
        add(f"lr_atr_cross_down_{period}", ((z.shift(1) > -0.7) & (z <= -0.7)).astype(float))

    for trend_period in (50, 80, 120):
        ema_trend = close.ewm(span=trend_period, adjust=False, min_periods=trend_period).mean()
        trend_delta = ema_trend.diff()
        add(f"trend_dir_{trend_period}", np.sign(trend_delta))
        add(f"trend_strength_{trend_period}", trend_delta / (out["atr_20"] + 1e-12))

    for bb_window, bb_std in ((13, 1.2), (13, 1.8), (20, 2.0)):
        middle = close.rolling(bb_window, min_periods=bb_window).mean()
        std = close.rolling(bb_window, min_periods=bb_window).std(ddof=0)
        upper = middle + bb_std * std
        lower = middle - bb_std * std
        tag = f"bb_{bb_window}_{str(bb_std).replace('.', '_')}"
        add(f"{tag}_width", (upper - lower) / (middle + 1e-12))
        add(f"{tag}_pos", (close - middle) / (upper - lower + 1e-12))
        add(f"{tag}_break_up", (close > upper).astype(float))
        add(f"{tag}_break_down", (close < lower).astype(float))

    add("body_pct", (out["close"] - out["open"]).abs() / (close + 1e-12))
    add("upper_wick_pct", (out["high"] - out[["open", "close"]].max(axis=1)) / (close + 1e-12))
    add("lower_wick_pct", (out[["open", "close"]].min(axis=1) - out["low"]) / (close + 1e-12))
    add("close_to_high", (out["high"] - out["close"]) / (close + 1e-12))
    add("close_to_low", (out["close"] - out["low"]) / (close + 1e-12))
    add("intrabar_return", (out["close"] - out["open"]) / (out["open"] + 1e-12))

    for window in (10, 20, 50, 100):
        volume_mean = volume.rolling(window, min_periods=window).mean()
        volume_std = volume.rolling(window, min_periods=window).std(ddof=0)
        add(f"volume_z_{window}", (volume - volume_mean) / (volume_std + 1e-12))
        add(f"relative_volume_{window}", volume / (volume_mean + 1e-12))

    local_time = out["event_time"].dt.tz_convert(config.timezone)
    add("hour", local_time.dt.hour.astype(float))
    add("minute", local_time.dt.minute.astype(float))
    add("bar_of_day", (local_time.dt.hour * 60 + local_time.dt.minute).astype(float))
    add("morning_session", (local_time.dt.hour < 12).astype(float))
    add("afternoon_session", (local_time.dt.hour >= 12).astype(float))

    # Consolidate the frame after creating the wide feature matrix. This keeps
    # pandas operations efficient for the many generated columns.
    out = out.copy()

    # The shift is deliberately performed after all rolling calculations. It is
    # the central leakage guard for Phase 06.
    out[generated] = out[generated].shift(config.shift_bars)
    out[generated] = out[generated].replace([np.inf, -np.inf], np.nan)
    out["feature_set_version"] = config.feature_set_version
    out["available_at"] = out["event_time"]
    out["feature_status"] = np.where(out[generated].notna().all(axis=1), "ready", "warmup")
    return out, generated


def build_features(
    frame: pd.DataFrame,
    *,
    config: FeatureConfig | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Build technical features grouped by symbol without future leakage."""

    cfg = config or FeatureConfig()
    source = _validate_input(frame)
    outputs: list[pd.DataFrame] = []
    feature_columns: list[str] = []
    for _, group in source.groupby("symbol", sort=False):
        built, feature_columns = _build_symbol_features(group.reset_index(drop=True), cfg)
        outputs.append(built)
    result = pd.concat(outputs, ignore_index=True).sort_values(["symbol", "event_time"]).reset_index(drop=True)
    return result, feature_columns


def build_features_from_parquet(
    input_root: str | Path,
    *,
    config: FeatureConfig | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Read a Silver Parquet dataset and build Gold features."""

    path = Path(input_root).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Silver Parquet input not found: {path}")
    return build_features(pd.read_parquet(path), config=config)


def write_features_parquet(
    frame: pd.DataFrame,
    output_root: str | Path,
    *,
    feature_columns: Iterable[str],
) -> Path:
    """Write Gold features as partitioned Parquet."""

    required = set(REQUIRED_COLUMNS) | {"feature_set_version", "available_at", "feature_status"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise FeatureContractError(f"Missing feature output columns: {missing}")
    root = Path(output_root).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    version = str(frame["feature_set_version"].iloc[0])
    safe_version = re.sub(r"[^A-Za-z0-9_.-]", "_", version)
    table = pa.Table.from_pandas(frame, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=root,
        partition_cols=["symbol", "timeframe", "trading_date"],
        basename_template=f"part-{safe_version}-{{i}}.parquet",
        existing_data_behavior="overwrite_or_ignore",
        max_partitions=4096,
    )
    return root
