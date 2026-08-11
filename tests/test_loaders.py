from __future__ import annotations

from pathlib import Path

import pandas as pd

from vn30f1m_dataset.loaders import (
    DataContractError,
    load_historical_csv,
    resample_ohlcv,
    validate_ohlcv,
)


FIXTURE = Path(__file__).parent / "fixtures" / "historical.csv"


def test_load_historical_csv_normalizes_legacy_columns():
    frame = load_historical_csv(FIXTURE)

    assert list(frame.columns) == [
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
    ]
    assert len(frame) == 5
    assert str(frame["event_time"].dt.tz) == "UTC"
    assert frame["event_time"].iloc[0] == pd.Timestamp("2026-01-05 02:00:00+00:00")
    assert frame["symbol"].unique().tolist() == ["VN30F1M"]
    assert frame["timeframe"].unique().tolist() == ["1m"]
    validate_ohlcv(frame)


def test_resample_uses_ohlcv_aggregation_and_keeps_gaps():
    raw = load_historical_csv(FIXTURE)
    bars = resample_ohlcv(raw, "15m")

    assert len(bars) == 2
    assert bars["open"].tolist() == [100.0, 102.0]
    assert bars["high"].tolist() == [103.0, 105.0]
    assert bars["low"].tolist() == [99.0, 101.5]
    assert bars["close"].tolist() == [102.0, 104.0]
    assert bars["volume"].tolist() == [60.0, 90.0]
    assert bars["timeframe"].unique().tolist() == ["15m"]
    validate_ohlcv(bars)


def test_invalid_ohlcv_is_rejected():
    raw = load_historical_csv(FIXTURE)
    raw.loc[0, "high"] = 1.0

    try:
        validate_ohlcv(raw)
    except DataContractError as exc:
        assert "High" in str(exc)
    else:
        raise AssertionError("invalid OHLCV row was accepted")
