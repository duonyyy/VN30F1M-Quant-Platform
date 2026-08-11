from __future__ import annotations

from pathlib import Path

import pytest

from vn30f1m_core.settings import Settings


FIXTURE_ROOT = Path(__file__).parent / "fixtures"


def test_default_settings_match_phase_01():
    settings = Settings.from_env(FIXTURE_ROOT / "missing-root")

    assert settings.default_symbol == "VN30F1M"
    assert settings.source_timeframe == "1m"
    assert settings.canonical_timeframe == "15m"
    assert settings.timezone == "Asia/Ho_Chi_Minh"
    assert settings.kafka_enabled is True
    assert settings.kafka_raw_topic == "vn30f1m.ohlcv.raw"


def test_dotenv_values_are_loaded(monkeypatch):
    settings = Settings.from_env(FIXTURE_ROOT)

    assert settings.canonical_timeframe == "5m"
    assert settings.kafka_enabled is False
    assert settings.project_name == "Fixture Platform"

    monkeypatch.setenv("VN30F1M_CANONICAL_TIMEFRAME", "30m")
    assert Settings.from_env(FIXTURE_ROOT).canonical_timeframe == "30m"


def test_invalid_timeframe_is_rejected(monkeypatch):
    monkeypatch.setenv("VN30F1M_CANONICAL_TIMEFRAME", "2m")

    with pytest.raises(ValueError, match="Unsupported canonical_timeframe"):
        Settings.from_env(FIXTURE_ROOT / "missing-root")
