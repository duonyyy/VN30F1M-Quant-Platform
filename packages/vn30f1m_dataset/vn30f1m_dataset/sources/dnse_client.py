"""DNSE market-data adapter.

The adapter intentionally stops at a normalized pandas DataFrame. Kafka
publishing belongs to Phase 05, so this module can be tested without a broker.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from typing import Any, Mapping

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOGGER = logging.getLogger(__name__)
DEFAULT_OPENAPI_URL = "https://openapi.dnse.com.vn"
DEFAULT_PUBLIC_URL = "https://api.dnse.com.vn"
DEFAULT_API_VERSION = "2026-05-07"
OHLC_ENDPOINT = "/chart-api/v2/ohlcs/futures"
ALLOWED_RESOLUTIONS = frozenset({"1", "5", "15", "30"})


class DNSEClientError(RuntimeError):
    """Base error raised by the DNSE adapter."""


class DNSEPayloadError(DNSEClientError):
    """Raised when DNSE returns a response outside the expected OHLC shape."""


@dataclass(frozen=True, slots=True)
class DNSEClientConfig:
    """Connection settings for DNSE, with no credentials baked into code."""

    api_key: str = ""
    api_secret: str = ""
    api_token: str = ""
    openapi_base_url: str = DEFAULT_OPENAPI_URL
    public_base_url: str = DEFAULT_PUBLIC_URL
    api_version: str = DEFAULT_API_VERSION
    timeout_seconds: float = 30.0
    max_retries: int = 3
    backoff_factor: float = 0.5

    @classmethod
    def from_settings(cls, settings: Any) -> "DNSEClientConfig":
        """Build a client config from ``vn30f1m_core.settings.Settings``."""

        return cls(
            api_key=settings.dnse_api_key,
            api_secret=settings.dnse_api_secret,
            api_token=settings.dnse_api_token,
            openapi_base_url=settings.dnse_openapi_url,
            public_base_url=settings.dnse_public_url,
            api_version=settings.dnse_api_version,
            timeout_seconds=settings.dnse_timeout_seconds,
            max_retries=settings.dnse_max_retries,
            backoff_factor=settings.dnse_backoff_factor,
        )

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero")
        if self.max_retries < 0:
            raise ValueError("max_retries must not be negative")
        if self.backoff_factor < 0:
            raise ValueError("backoff_factor must not be negative")


class DNSEClient:
    """Small synchronous DNSE client for VN30F1M market data."""

    def __init__(
        self,
        config: DNSEClientConfig | None = None,
        *,
        session: requests.Session | None = None,
    ) -> None:
        self.config = config or DNSEClientConfig()
        self.session = session or requests.Session()
        self._configure_retries()

    def _configure_retries(self) -> None:
        retry = Retry(
            total=self.config.max_retries,
            connect=self.config.max_retries,
            read=self.config.max_retries,
            status=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=(408, 429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "HEAD", "OPTIONS"}),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "DNSEClient":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()

    @staticmethod
    def _clean_base_url(url: str) -> str:
        return url.rstrip("/")

    def _build_signature(
        self,
        method: str,
        path_with_query: str,
        date_value: str,
        nonce: str | None = None,
    ) -> str:
        """Build the legacy DNSE HMAC-SHA256 signature format."""

        if not self.config.api_secret:
            raise DNSEClientError("DNSE_API_SECRET is required for authenticated requests")

        signing_lines = [
            f"(request-target): {method.lower()} {path_with_query}",
            f"date: {date_value}",
        ]
        if nonce:
            signing_lines.append(f"nonce: {nonce}")
        signing_string = "\n".join(signing_lines)
        digest = hmac.new(
            self.config.api_secret.encode("utf-8"),
            signing_string.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(digest).decode("ascii")

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        use_openapi: bool = False,
    ) -> Any:
        if not path.startswith("/"):
            raise ValueError("DNSE path must start with '/'")

        base_url = self.config.openapi_base_url if use_openapi else self.config.public_base_url
        url = f"{self._clean_base_url(base_url)}{path}"
        headers = {"Accept": "application/json"}
        if self.config.api_version:
            headers["version"] = self.config.api_version

        if use_openapi:
            if not self.config.api_key or not self.config.api_secret:
                raise DNSEClientError(
                    "DNSE_API_KEY and DNSE_API_SECRET are required for authenticated requests"
                )
            date_value = format_datetime(datetime.now(timezone.utc), usegmt=True)
            nonce = str(uuid.uuid4())
            query_string = requests.models.RequestEncodingMixin._encode_params(params or {})
            signed_path = path + (f"?{query_string}" if query_string else "")
            headers.update(
                {
                    "Date": date_value,
                    "x-api-key": self.config.api_key,
                    "X-Signature": self._build_signature(
                        method, signed_path, date_value, nonce
                    ),
                    "X-Nonce": nonce,
                }
            )
        elif self.config.api_token:
            headers["Authorization"] = f"Bearer {self.config.api_token}"

        LOGGER.info("DNSE request method=%s path=%s authenticated=%s", method, path, use_openapi)
        try:
            response = self.session.request(
                method=method,
                url=url,
                params=dict(params or {}),
                headers=headers,
                timeout=self.config.timeout_seconds,
            )
            if response.status_code >= 400:
                raise DNSEClientError(
                    f"DNSE request failed with HTTP status {response.status_code}"
                )
            try:
                return response.json()
            except ValueError as exc:
                raise DNSEClientError("DNSE response was not valid JSON") from exc
        except DNSEClientError:
            raise
        except requests.RequestException as exc:
            raise DNSEClientError(f"DNSE request failed: {exc.__class__.__name__}") from exc

    @staticmethod
    def _resolution(value: str | int) -> tuple[str, str]:
        raw = str(value).strip().lower()
        if raw.endswith("m"):
            raw = raw[:-1]
        if raw not in ALLOWED_RESOLUTIONS:
            allowed = ", ".join(sorted(ALLOWED_RESOLUTIONS, key=int))
            raise ValueError(f"Unsupported DNSE resolution {value!r}; expected one of {allowed}")
        return raw, f"{raw}m"

    @staticmethod
    def _timestamp(value: int | float | datetime | pd.Timestamp) -> int:
        if isinstance(value, (datetime, pd.Timestamp)):
            timestamp = pd.Timestamp(value)
            if timestamp.tzinfo is None:
                timestamp = timestamp.tz_localize("UTC")
            else:
                timestamp = timestamp.tz_convert("UTC")
            return int(timestamp.timestamp())
        return int(value)

    @staticmethod
    def _timestamp_series(values: Any) -> pd.Series:
        series = pd.Series(values)
        numeric = pd.to_numeric(series, errors="coerce")
        if numeric.notna().all():
            unit = "ms" if float(numeric.abs().max()) >= 100_000_000_000 else "s"
            parsed = pd.to_datetime(numeric, unit=unit, utc=True, errors="coerce")
        else:
            parsed = pd.to_datetime(series, utc=True, errors="coerce")
        if parsed.isna().any():
            raise DNSEPayloadError("DNSE OHLC payload contains an invalid timestamp")
        return parsed

    @staticmethod
    def _extract_records(payload: Any) -> dict[str, list[Any]]:
        if isinstance(payload, dict) and "data" in payload:
            payload = payload["data"]

        if isinstance(payload, dict):
            aliases = {
                "t": ("t", "timestamp", "time"),
                "o": ("o", "open"),
                "h": ("h", "high"),
                "l": ("l", "low"),
                "c": ("c", "close"),
                "v": ("v", "volume"),
            }
            result: dict[str, list[Any]] = {}
            for canonical, names in aliases.items():
                for name in names:
                    if name in payload:
                        value = payload[name]
                        result[canonical] = list(value) if not np.isscalar(value) else [value]
                        break
            return result

        if isinstance(payload, list) and all(isinstance(row, dict) for row in payload):
            aliases = {
                "t": ("t", "timestamp", "time"),
                "o": ("o", "open"),
                "h": ("h", "high"),
                "l": ("l", "low"),
                "c": ("c", "close"),
                "v": ("v", "volume"),
            }
            result = {}
            for canonical, names in aliases.items():
                for name in names:
                    if name in payload[0]:
                        result[canonical] = [row.get(name) for row in payload]
                        break
            return result

        raise DNSEPayloadError("DNSE OHLC response must be an object or a list of records")

    def _normalize_ohlcv(
        self,
        payload: Any,
        *,
        symbol: str,
        timeframe: str,
    ) -> pd.DataFrame:
        records = self._extract_records(payload)
        required = {"t", "o", "h", "l", "c"}
        missing = sorted(required - records.keys())
        if missing:
            raise DNSEPayloadError(f"DNSE OHLC response is missing fields: {', '.join(missing)}")

        lengths = {key: len(value) for key, value in records.items() if key in required}
        if len(set(lengths.values())) != 1:
            raise DNSEPayloadError("DNSE OHLC arrays have inconsistent lengths")
        row_count = next(iter(lengths.values()))
        if row_count == 0:
            return self._empty_frame()

        frame = pd.DataFrame(
            {
                "event_time": self._timestamp_series(records["t"]),
                "open": pd.to_numeric(records["o"], errors="coerce"),
                "high": pd.to_numeric(records["h"], errors="coerce"),
                "low": pd.to_numeric(records["l"], errors="coerce"),
                "close": pd.to_numeric(records["c"], errors="coerce"),
                "volume": pd.to_numeric(records.get("v", [0.0] * row_count), errors="coerce"),
            }
        )
        numeric_columns = ["open", "high", "low", "close", "volume"]
        if frame[numeric_columns].isna().any().any():
            raise DNSEPayloadError("DNSE OHLC response contains non-numeric values")
        if not np.isfinite(frame[numeric_columns].to_numpy(dtype=float)).all():
            raise DNSEPayloadError("DNSE OHLC response contains non-finite values")
        if (frame["volume"] < 0).any():
            raise DNSEPayloadError("DNSE OHLC response contains negative volume")
        if ((frame["high"] < frame[["open", "close", "low"]].max(axis=1)) | (frame["low"] > frame[["open", "close", "high"]].min(axis=1))).any():
            raise DNSEPayloadError("DNSE OHLC response violates OHLC bounds")

        frame = frame.sort_values("event_time").reset_index(drop=True)
        if frame["event_time"].duplicated().any():
            raise DNSEPayloadError("DNSE OHLC response contains duplicate timestamps")
        frame.insert(0, "symbol", symbol.upper())
        frame.insert(2, "trading_date", frame["event_time"].dt.tz_convert("Asia/Ho_Chi_Minh").dt.date.astype(str))
        frame.insert(3, "timeframe", timeframe)
        frame["source"] = "dnse_api"
        frame["source_record_id"] = frame["event_time"].map(
            lambda timestamp: f"dnse:{symbol.upper()}:{timestamp.isoformat()}"
        )
        return frame[
            [
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
            ]
        ]

    @staticmethod
    def _empty_frame() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "symbol", "event_time", "trading_date", "timeframe", "open", "high",
                "low", "close", "volume", "source", "source_record_id",
            ]
        )

    def get_ohlcv_futures(
        self,
        symbol: str = "VN30F1M",
        from_timestamp: int | float | datetime | pd.Timestamp | None = None,
        to_timestamp: int | float | datetime | pd.Timestamp | None = None,
        resolution: str | int = "1",
    ) -> pd.DataFrame:
        """Fetch and normalize futures OHLCV from the public DNSE market-data API."""

        clean_symbol = symbol.strip().upper()
        if not clean_symbol:
            raise ValueError("symbol must not be empty")
        api_resolution, timeframe = self._resolution(resolution)
        now = datetime.now(timezone.utc)
        end = self._timestamp(to_timestamp or now)
        start = self._timestamp(from_timestamp or (now - timedelta(days=30)))
        if start > end:
            raise ValueError("from_timestamp must not be later than to_timestamp")

        payload = self._request_json(
            "GET",
            OHLC_ENDPOINT,
            params={
                "symbol": clean_symbol,
                "from": start,
                "to": end,
                "resolution": api_resolution,
            },
            use_openapi=False,
        )
        return self._normalize_ohlcv(payload, symbol=clean_symbol, timeframe=timeframe)
