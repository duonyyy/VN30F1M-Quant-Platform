from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from vn30f1m_streaming import (
    KafkaOHLCVConsumer,
    KafkaOHLCVProducer,
    build_ohlcv_event,
    kafka_message_key,
)


class FakeFuture:
    def get(self, timeout: int) -> None:
        return None


class FakeProducer:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str, object]] = []
        self.flushed = 0

    def send(self, topic: str, *, key: str, value: object) -> FakeFuture:
        self.messages.append((topic, key, value))
        return FakeFuture()

    def flush(self) -> None:
        self.flushed += 1

    def close(self) -> None:
        return None


class FakeConsumer:
    def __init__(self, messages: list[object]) -> None:
        self.messages = messages
        self.commits = 0

    def poll(self, *, timeout_ms: int, max_records: int) -> dict[int, list[object]]:
        messages, self.messages = self.messages, []
        return {0: messages[:max_records]} if messages else {}

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        return None


class FakeKeyStore:
    def __init__(self) -> None:
        self.keys: set[str] = set()

    def claim(self, key: str) -> bool:
        if key in self.keys:
            return False
        self.keys.add(key)
        return True

    def release(self, key: str) -> None:
        self.keys.discard(key)

    def close(self) -> None:
        return None


class InMemoryConsumer(KafkaOHLCVConsumer):
    def __init__(self, *args: object, **kwargs: object) -> None:
        self.written: list[dict[str, object]] = []
        super().__init__(*args, **kwargs)

    def _write_bronze(self, event: object, message: object, key: str) -> None:
        self.written.append({"event": event, "key": key})


def _row() -> dict[str, object]:
    return {
        "symbol": "VN30F1M",
        "event_time": "2026-08-06T02:15:00Z",
        "timeframe": "1m",
        "open": 1234.5,
        "high": 1236.0,
        "low": 1234.0,
        "close": 1235.2,
        "volume": 1000.0,
        "source": "test",
    }


def test_producer_uses_contract_key_and_json_event():
    fake = FakeProducer()
    producer = KafkaOHLCVProducer(producer=fake)

    count = producer.send_rows([_row()], source="fixture")

    assert count == 1
    assert fake.flushed == 1
    topic, key, event = fake.messages[0]
    assert topic == "vn30f1m.ohlcv.raw"
    assert key == "VN30F1M|2026-08-06T02:15:00Z|1m"
    assert key == kafka_message_key(event)
    assert event["schema_version"] == "ohlcv_raw_v1"
    assert event["trading_date"] == "2026-08-06"


def test_consumer_is_idempotent_and_routes_invalid_event_to_dlq():
    event = build_ohlcv_event(_row())
    key = kafka_message_key(event)
    valid = SimpleNamespace(key=key, value=json.dumps(event).encode(), partition=0, offset=1)
    duplicate = SimpleNamespace(key=key, value=json.dumps(event).encode(), partition=0, offset=2)
    invalid = SimpleNamespace(key="wrong-key", value=b"{\"bad\": true}", partition=0, offset=3)
    fake_consumer = FakeConsumer([valid, duplicate, invalid])
    fake_dlq = FakeProducer()

    consumer = InMemoryConsumer(
        consumer=fake_consumer,
        dlq_producer=fake_dlq,
        bronze_root=Path("lakehouse/bronze"),
        key_store=FakeKeyStore(),
    )
    summary = consumer.run_once(timeout_ms=1, max_records=10)
    consumer.close()

    assert summary == {"accepted": 1, "duplicate": 1, "dead_lettered": 1}
    assert fake_consumer.commits == 1
    assert len(consumer.written) == 1
    assert len(fake_dlq.messages) == 1
    assert fake_dlq.messages[0][0] == "vn30f1m.ohlcv.raw.dlq"
    json.dumps(fake_dlq.messages[0][2])
