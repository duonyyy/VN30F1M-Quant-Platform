"""Kafka ingestion primitives for the VN30F1M platform."""

from .kafka_io import (
    DLQ_TOPIC,
    RAW_TOPIC,
    KafkaEventError,
    KafkaOHLCVConsumer,
    KafkaOHLCVProducer,
    StreamingError,
    build_ohlcv_event,
    kafka_message_key,
    validate_ohlcv_event,
)

__all__ = [
    "DLQ_TOPIC",
    "RAW_TOPIC",
    "KafkaEventError",
    "KafkaOHLCVConsumer",
    "KafkaOHLCVProducer",
    "StreamingError",
    "build_ohlcv_event",
    "kafka_message_key",
    "validate_ohlcv_event",
]
