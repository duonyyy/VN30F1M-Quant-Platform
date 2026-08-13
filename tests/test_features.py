from __future__ import annotations

import numpy as np
import pandas as pd

from vn30f1m_analysis.ml.features import FeatureConfig, build_features


def _silver_frame(rows: int = 180) -> pd.DataFrame:
    event_time = pd.date_range("2024-01-01 02:00:00Z", periods=rows, freq="15min")
    close = 1200 + np.arange(rows, dtype=float) * 0.7 + np.sin(np.arange(rows) / 4) * 3
    return pd.DataFrame(
        {
            "symbol": "VN30F1M",
            "event_time": event_time,
            "trading_date": event_time.tz_convert("Asia/Ho_Chi_Minh").date,
            "timeframe": "15m",
            "open": close - 0.4,
            "high": close + 1.2,
            "low": close - 1.1,
            "close": close,
            "volume": 1000 + np.arange(rows) * 2,
        }
    )


def test_build_features_returns_versioned_gold_rows():
    frame, feature_columns = build_features(_silver_frame())

    assert len(frame) == 180
    assert len(feature_columns) > 100
    assert frame["feature_set_version"].eq("vn30f1m_features_v1").all()
    assert frame["available_at"].dt.tz is not None
    assert set(frame["feature_status"]) == {"warmup", "ready"}
    assert frame.loc[frame["feature_status"] == "ready", feature_columns].notna().all().all()
    assert "future_return" not in frame.columns
    assert "label" not in frame.columns


def test_feature_at_t_does_not_change_when_only_current_bar_changes():
    original = _silver_frame()
    changed = original.copy()
    changed.loc[120, "close"] = changed.loc[120, "close"] + 500
    changed.loc[120, "open"] = changed.loc[120, "close"] - 0.4
    changed.loc[120, "high"] = changed.loc[120, "close"] + 1.2
    changed.loc[120, "low"] = changed.loc[120, "close"] - 1.1

    first, columns = build_features(original, config=FeatureConfig(shift_bars=1))
    second, _ = build_features(changed, config=FeatureConfig(shift_bars=1))

    # Row t uses the feature calculation from t-1. A change at t must only
    # affect later feature rows, never the feature vector at t itself.
    assert np.allclose(
        first.loc[120, columns].to_numpy(dtype=float),
        second.loc[120, columns].to_numpy(dtype=float),
        equal_nan=True,
    )
    assert not np.allclose(
        first.loc[121, columns].to_numpy(dtype=float),
        second.loc[121, columns].to_numpy(dtype=float),
        equal_nan=True,
    )


def test_features_are_isolated_between_symbols():
    first = _silver_frame(140)
    second = first.copy()
    second["symbol"] = "VN30F2M"
    second["close"] = second["close"] * 2
    second["open"] = second["close"] - 0.4
    second["high"] = second["close"] + 1.2
    second["low"] = second["close"] - 1.1
    frame, columns = build_features(pd.concat([first, second], ignore_index=True))

    for symbol in ("VN30F1M", "VN30F2M"):
        rows = frame[frame["symbol"] == symbol]
        assert len(rows) == 140
        assert rows[columns].iloc[0].isna().all()
        assert rows[columns].iloc[-1].notna().all()
