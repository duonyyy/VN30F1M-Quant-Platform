"""Kafka producer/consumer for the canonical VN30F1M OHLCV event.

Kafka imports are lazy so the core and batch packages remain usable without
installing the optional streaming dependency.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping
from uuid import uuid4

import pandas as pd

LOGGER = logging.getLogger(__name__)
RAW_TOPIC = "vn30f1m.ohlcv.raw"
DLQ_TOPIC = "vn30f1m.ohlcv.raw.dlq"
RAW_SCHEMA_VERSION = "ohlcv_raw_v1"
DLQ_SCHEMA_VERSION = "ohlcv_raw_dlq_v1"
LOCAL_MARKET_TIMEZONE = "Asia/Ho_Chi_Minh"


class StreamingError(RuntimeError):
    """Base error for the Kafka ingestion layer."""


class KafkaEventError(StreamingError, ValueError):
    """Raised when an event violates the raw OHLCV contract."""


def _utc_iso(value: Any) -> str:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.isoformat().replace("+00:00", "Z")


def _trading_date(event_time: str) -> str:
    return str(pd.Timestamp(event_time).tz_convert(LOCAL_MARKET_TIMEZONE).date())


def kafka_message_key(event: Mapping[str, Any]) -> str:
    return f"{event['symbol']}|{event['event_time']}|{event['timeframe']}"


def build_ohlcv_event(
    row: Mapping[str, Any],
    *,
    source: str | None = None,
    ingest_run_id: str | None = None,
    published_at: Any | None = None,
) -> dict[str, Any]:
    """Convert a loader/DNSE row into the Kafka raw event schema."""

    try:
        symbol = str(row["symbol"]).strip().upper()
        event_time = _utc_iso(row["event_time"])
        timeframe = str(row["timeframe"]).strip()
        event = {
            "schema_version": RAW_SCHEMA_VERSION,
            "symbol": symbol,
            "event_time": event_time,
            "trading_date": str(row.get("trading_date") or _trading_date(event_time)),
            "timeframe": timeframe,
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["volume"]),
            "source": str(source or row.get("source") or "unknown"),
            "source_record_id": row.get("source_record_id"),
            "ingest_run_id": str(ingest_run_id or row.get("ingest_run_id") or uuid4()),
            "published_at": _utc_iso(published_at or datetime.now(timezone.utc)),
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise KafkaEventError(f"Unable to build OHLCV event: {exc}") from exc

    validate_ohlcv_event(event)
    return event


def validate_ohlcv_event(event: Mapping[str, Any], message_key: str | None = None) -> None:
    """Validate schema, timestamps, OHLC bounds, volume and business key."""

    required = {
        "schema_version", "symbol", "event_time", "trading_date", "timeframe",
        "open", "high", "low", "close", "volume", "source", "ingest_run_id",
        "published_at",
    }
    missing = sorted(required - set(event))
    if missing:
        raise KafkaEventError(f"Missing event fields: {', '.join(missing)}")
    if event["schema_version"] != RAW_SCHEMA_VERSION:
        raise KafkaEventError(f"Unsupported schema_version: {event['schema_version']!r}")
    if not str(event["symbol"]).strip() or not str(event["timeframe"]).strip():
        raise KafkaEventError("symbol and timeframe must not be empty")

    try:
        event_timestamp = pd.Timestamp(event["event_time"])
        published_timestamp = pd.Timestamp(event["published_at"])
        if event_timestamp.tzinfo is None or published_timestamp.tzinfo is None:
            raise ValueError("timestamps must be timezone-aware")
        numeric = [float(event[column]) for column in ("open", "high", "low", "close", "volume")]
    except (TypeError, ValueError) as exc:
        raise KafkaEventError(f"Invalid event value: {exc}") from exc

    if any(pd.isna(value) or not pd.api.types.is_number(value) for value in numeric):
        raise KafkaEventError("OHLCV values must be finite numbers")
    if any(value != value or value in {float("inf"), float("-inf")} for value in numeric):
        raise KafkaEventError("OHLCV values must be finite numbers")
    open_price, high, low, close, volume = numeric
    if min(open_price, high, low, close) <= 0:
        raise KafkaEventError("OHLC prices must be greater than zero")
    if high < max(open_price, close, low) or low > min(open_price, close, high):
        raise KafkaEventError("OHLC bounds are invalid")
    if volume < 0:
        raise KafkaEventError("volume must not be negative")
    if str(event["trading_date"]) != _trading_date(_utc_iso(event_timestamp)):
        raise KafkaEventError("trading_date does not match event_time")
    if message_key is not None and message_key != kafka_message_key(event):
        raise KafkaEventError("Kafka message key does not match the event business key")


def _load_kafka_producer(bootstrap_servers: str, **kwargs: Any) -> Any:
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise StreamingError(
            "kafka-python is required; install the streaming extra with "
            "python -m pip install -e .[streaming]"
        ) from exc
    return KafkaProducer(bootstrap_servers=bootstrap_servers, **kwargs)


def _load_kafka_consumer(bootstrap_servers: str, topic: str, group_id: str, **kwargs: Any) -> Any:
    try:
        from kafka import KafkaConsumer
    except ImportError as exc:
        raise StreamingError(
            "kafka-python is required; install the streaming extra with "
            "python -m pip install -e .[streaming]"
        ) from exc
    return KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, group_id=group_id, **kwargs)


class KafkaOHLCVProducer:
    """Publish validated OHLCV events with deterministic Kafka keys."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = RAW_TOPIC,
        *,
        producer: Any | None = None,
        producer_factory: Callable[..., Any] | None = None,
    ) -> None:
        self.topic = topic
        if producer is not None:
            self.producer = producer
        else:
            factory = producer_factory or _load_kafka_producer
            self.producer = factory(
                bootstrap_servers=bootstrap_servers,
                acks="all",
                retries=5,
                key_serializer=lambda value: value.encode("utf-8"),
                value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
            )

    @classmethod
    def from_settings(cls, settings: Any, **kwargs: Any) -> "KafkaOHLCVProducer":
        return cls(settings.kafka_bootstrap_servers, settings.kafka_raw_topic, **kwargs)

    def send_event(self, event: Mapping[str, Any], *, wait: bool = True) -> str:
        event_copy = dict(event)
        validate_ohlcv_event(event_copy)
        key = kafka_message_key(event_copy)
        future = self.producer.send(self.topic, key=key, value=event_copy)
        if wait and future is not None and hasattr(future, "get"):
            future.get(timeout=30)
        return key

    def send_rows(
        self,
        rows: Iterable[Mapping[str, Any]],
        *,
        source: str | None = None,
        ingest_run_id: str | None = None,
    ) -> int:
        count = 0
        for row in rows:
            self.send_event(
                build_ohlcv_event(row, source=source, ingest_run_id=ingest_run_id),
                wait=False,
            )
            count += 1
        self.flush()
        return count

    def send_dataframe(
        self,
        frame: pd.DataFrame,
        *,
        source: str | None = None,
        ingest_run_id: str | None = None,
    ) -> int:
        return self.send_rows(
            frame.to_dict(orient="records"), source=source, ingest_run_id=ingest_run_id
        )

    def send_csv(
        self,
        input_path: str | Path,
        *,
        symbol: str = "VN30F1M",
        source_timezone: str = LOCAL_MARKET_TIMEZONE,
        limit: int | None = None,
    ) -> int:
        from vn30f1m_dataset.loaders import load_historical_csv

        frame = load_historical_csv(
            Path(input_path), symbol=symbol, source_timezone=source_timezone
        )
        if limit is not None:
            if limit <= 0:
                raise ValueError("limit must be greater than zero")
            frame = frame.head(limit)
        return self.send_dataframe(frame, source="historical_csv")

    def flush(self) -> None:
        self.producer.flush()

    def close(self) -> None:
        self.producer.close()

    def __enter__(self) -> "KafkaOHLCVProducer":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()


class _ProcessedKeyStore:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS processed_keys "
            "(message_key TEXT PRIMARY KEY, processed_at TEXT NOT NULL)"
        )
        self.connection.commit()

    def claim(self, key: str) -> bool:
        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO processed_keys(message_key, processed_at) VALUES (?, ?)",
            (key, datetime.now(timezone.utc).isoformat()),
        )
        self.connection.commit()
        return cursor.rowcount == 1

    def release(self, key: str) -> None:
        self.connection.execute("DELETE FROM processed_keys WHERE message_key = ?", (key,))
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()


class KafkaOHLCVConsumer:
    """Consume raw events, write bronze JSONL, and route bad records to DLQ."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = RAW_TOPIC,
        group_id: str = "vn30f1m-batch-consumer",
        bronze_root: str | Path = "lakehouse/bronze/vn30f1m/ohlcv_raw",
        *,
        dlq_topic: str = DLQ_TOPIC,
        consumer: Any | None = None,
        dlq_producer: Any | None = None,
        consumer_factory: Callable[..., Any] | None = None,
        producer_factory: Callable[..., Any] | None = None,
        key_store: Any | None = None,
    ) -> None:
        self.topic = topic
        self.dlq_topic = dlq_topic
        self.bronze_root = Path(bronze_root)
        self.bronze_root.mkdir(parents=True, exist_ok=True)
        self._store = key_store or _ProcessedKeyStore(
            self.bronze_root / "_consumer_state" / "processed.sqlite3"
        )
        if consumer is not None:
            self.consumer = consumer
        else:
            factory = consumer_factory or _load_kafka_consumer
            self.consumer = factory(
                bootstrap_servers=bootstrap_servers,
                topic=topic,
                group_id=group_id,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                key_deserializer=lambda value: value.decode("utf-8"),
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            )
        if dlq_producer is not None:
            self.dlq_producer = dlq_producer
        else:
            factory = producer_factory or _load_kafka_producer
            self.dlq_producer = factory(
                bootstrap_servers=bootstrap_servers,
                acks="all",
                retries=5,
                key_serializer=lambda value: value.encode("utf-8"),
                value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
            )

    @classmethod
    def from_settings(cls, settings: Any, **kwargs: Any) -> "KafkaOHLCVConsumer":
        return cls(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_raw_topic,
            group_id=settings.kafka_consumer_group,
            bronze_root=settings.paths.bronze / "vn30f1m" / "ohlcv_raw",
            **kwargs,
        )

    @staticmethod
    def _text_key(value: Any) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value)

    @staticmethod
    def _text_payload(value: Any) -> Any:
        if isinstance(value, bytes):
            return json.loads(value.decode("utf-8"))
        if isinstance(value, str):
            return json.loads(value)
        return value

    def _write_bronze(self, event: Mapping[str, Any], message: Any, key: str) -> None:
        partition = getattr(message, "partition", -1)
        offset = getattr(message, "offset", -1)
        output_dir = (
            self.bronze_root
            / f"symbol={event['symbol']}"
            / f"timeframe={event['timeframe']}"
            / f"trading_date={event['trading_date']}"
        )
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "events.jsonl"
        record = dict(event)
        record.update(
            {
                "kafka_key": key,
                "kafka_partition": partition,
                "kafka_offset": offset,
                "consumed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            }
        )
        with output_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _send_dlq(self, key: str, payload: Any, error: Exception) -> None:
        if isinstance(payload, bytes):
            safe_payload: Any = payload.decode("utf-8", errors="replace")
        elif isinstance(payload, (str, int, float, bool, type(None), list, dict)):
            safe_payload = payload
        else:
            safe_payload = repr(payload)
        record = {
            "schema_version": DLQ_SCHEMA_VERSION,
            "original_key": key,
            "error_type": error.__class__.__name__,
            "error": str(error),
            "payload": safe_payload,
            "failed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        future = self.dlq_producer.send(self.dlq_topic, key=key, value=record)
        if future is not None and hasattr(future, "get"):
            future.get(timeout=30)

    def process_message(self, message: Any) -> str:
        key = self._text_key(getattr(message, "key", ""))
        raw_payload = getattr(message, "value", None)
        try:
            payload = self._text_payload(raw_payload)
            if not isinstance(payload, Mapping):
                raise KafkaEventError("Kafka payload must be a JSON object")
            validate_ohlcv_event(payload, message_key=key)
            if not self._store.claim(key):
                return "duplicate"
            try:
                self._write_bronze(payload, message, key)
            except Exception:
                self._store.release(key)
                raise
            return "accepted"
        except Exception as exc:
            LOGGER.warning("Routing invalid Kafka event to DLQ key=%s error=%s", key, exc)
            self._send_dlq(key, raw_payload, exc)
            return "dead_lettered"

    def run_once(self, *, timeout_ms: int = 1000, max_records: int = 100) -> dict[str, int]:
        messages = self.consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
        summary = {"accepted": 0, "duplicate": 0, "dead_lettered": 0}
        for records in messages.values():
            for message in records:
                summary[self.process_message(message)] += 1
        if messages:
            self.consumer.commit()
        self.dlq_producer.flush()
        return summary

    def consume_forever(self) -> None:
        for message in self.consumer:
            result = self.process_message(message)
            if result in {"accepted", "duplicate", "dead_lettered"}:
                self.consumer.commit()

    def close(self) -> None:
        self.consumer.close()
        self.dlq_producer.close()
        self._store.close()

    def __enter__(self) -> "KafkaOHLCVConsumer":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()
