from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from vn30f1m_dataset.sources import DNSEClient, DNSEClientConfig, DNSEClientError, DNSEPayloadError


def _client_with_payload(payload: object, status_code: int = 200) -> tuple[DNSEClient, MagicMock]:
    session = MagicMock()
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = payload
    session.request.return_value = response
    return DNSEClient(DNSEClientConfig(max_retries=0), session=session), session


def test_get_ohlcv_futures_normalizes_public_payload_to_canonical_columns():
    client, session = _client_with_payload(
        {
            "t": [1722502800, 1722502860],
            "o": [1234, 1235],
            "h": [1236, 1237],
            "l": [1233, 1234],
            "c": [1235, 1236],
            "v": [10, 12],
        }
    )

    frame = client.get_ohlcv_futures(
        from_timestamp=datetime(2024, 8, 1, 2, 20, tzinfo=timezone.utc),
        to_timestamp=datetime(2024, 8, 1, 2, 30, tzinfo=timezone.utc),
        resolution="1m",
    )

    assert list(frame.columns) == [
        "symbol", "event_time", "trading_date", "timeframe", "open", "high",
        "low", "close", "volume", "source", "source_record_id",
    ]
    assert frame["symbol"].tolist() == ["VN30F1M", "VN30F1M"]
    assert frame["timeframe"].tolist() == ["1m", "1m"]
    assert str(frame["event_time"].dt.tz) == "UTC"
    assert frame["source"].tolist() == ["dnse_api", "dnse_api"]

    request = session.request.call_args.kwargs
    assert request["url"] == "https://api.dnse.com.vn/chart-api/v2/ohlcs/futures"
    assert request["params"]["symbol"] == "VN30F1M"
    assert request["params"]["resolution"] == "1"
    assert request["timeout"] == 30.0


def test_get_ohlcv_futures_supports_record_list_and_missing_volume():
    client, _ = _client_with_payload(
        [
            {"timestamp": 1722502800, "open": 1234, "high": 1236, "low": 1233, "close": 1235},
        ]
    )

    frame = client.get_ohlcv_futures(
        from_timestamp=1722502800,
        to_timestamp=1722502860,
        resolution=5,
    )

    assert frame.loc[0, "timeframe"] == "5m"
    assert frame.loc[0, "volume"] == 0.0


def test_http_failure_is_wrapped_without_exposing_credentials():
    client, _ = _client_with_payload({}, status_code=503)
    client.config = DNSEClientConfig(api_token="token-that-must-not-be-logged", max_retries=0)

    with pytest.raises(DNSEClientError, match="HTTP status 503"):
        client.get_ohlcv_futures(from_timestamp=1, to_timestamp=2)


def test_invalid_ohlc_payload_is_rejected():
    client, _ = _client_with_payload(
        {"t": [1722502800], "o": [1234], "h": [1230], "l": [1233], "c": [1235]}
    )

    with pytest.raises(DNSEPayloadError, match="OHLC bounds"):
        client.get_ohlcv_futures(from_timestamp=1, to_timestamp=2)


def test_settings_config_does_not_print_dnse_secrets(monkeypatch):
    from vn30f1m_core.settings import Settings

    monkeypatch.setenv("DNSE_API_KEY", "api-key")
    monkeypatch.setenv("DNSE_API_SECRET", "api-secret")
    monkeypatch.setenv("DNSE_API_TOKEN", "api-token")
    payload = Settings.from_env().as_dict()

    assert "dnse_api_secret" not in payload
    assert payload["dnse_api_key_configured"] is True
    assert payload["dnse_api_secret_configured"] is True
    assert payload["dnse_api_token_configured"] is True
