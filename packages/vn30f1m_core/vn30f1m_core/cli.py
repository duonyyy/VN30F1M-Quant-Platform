"""Command-line entry point for the VN30F1M platform."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .settings import Settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="vn30f1m", description="VN30F1M platform CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status = subparsers.add_parser("status", help="show project paths and runtime settings")
    status.add_argument("--root", type=Path, help="override the platform repository root")
    status.add_argument("--json", action="store_true", help="print machine-readable JSON")

    dataset = subparsers.add_parser("dataset", help="dataset ingestion commands")
    dataset_subparsers = dataset.add_subparsers(dest="dataset_command", required=True)
    load_historical = dataset_subparsers.add_parser(
        "load-historical", help="load, normalize and resample the legacy historical CSV"
    )
    load_historical.add_argument("--root", type=Path, help="override the platform repository root")
    load_historical.add_argument("--input", type=Path, help="historical CSV path")
    load_historical.add_argument("--output", type=Path, help="Parquet dataset output root")
    load_historical.add_argument("--symbol", help="instrument symbol; defaults to settings")
    load_historical.add_argument(
        "--source-timezone", help="timezone for naive CSV timestamps; defaults to settings"
    )
    load_historical.add_argument(
        "--timeframe", choices=("1m", "5m", "15m", "30m"), help="target output timeframe"
    )
    load_historical.add_argument("--json", action="store_true", help="print machine-readable JSON")

    stream = subparsers.add_parser("stream", help="Kafka streaming commands")
    stream_subparsers = stream.add_subparsers(dest="stream_command", required=True)
    publish_csv = stream_subparsers.add_parser(
        "publish-csv", help="publish canonical historical CSV rows to Kafka"
    )
    publish_csv.add_argument("--root", type=Path, help="override the platform repository root")
    publish_csv.add_argument("--input", type=Path, required=True, help="historical CSV path")
    publish_csv.add_argument("--symbol", help="instrument symbol; defaults to settings")
    publish_csv.add_argument("--source-timezone", help="timezone for naive CSV timestamps")
    publish_csv.add_argument("--limit", type=int, help="publish only the first N rows")
    publish_csv.add_argument("--json", action="store_true", help="print machine-readable JSON")

    publish_dnse = stream_subparsers.add_parser(
        "publish-dnse", help="fetch DNSE OHLCV and publish it to Kafka"
    )
    publish_dnse.add_argument("--root", type=Path, help="override the platform repository root")
    publish_dnse.add_argument("--symbol", help="instrument symbol; defaults to settings")
    publish_dnse.add_argument("--from-timestamp", type=int)
    publish_dnse.add_argument("--to-timestamp", type=int)
    publish_dnse.add_argument("--resolution", default="1", help="DNSE resolution: 1, 5, 15 or 30")
    publish_dnse.add_argument("--json", action="store_true", help="print machine-readable JSON")

    consume_once = stream_subparsers.add_parser(
        "consume-once", help="consume a bounded batch into local bronze and DLQ"
    )
    consume_once.add_argument("--root", type=Path, help="override the platform repository root")
    consume_once.add_argument("--output", type=Path, help="bronze output root")
    consume_once.add_argument("--timeout-ms", type=int, default=1000)
    consume_once.add_argument("--max-records", type=int, default=100)
    consume_once.add_argument("--json", action="store_true", help="print machine-readable JSON")

    batch = subparsers.add_parser("batch", help="Spark batch processing commands")
    batch_subparsers = batch.add_subparsers(dest="batch_command", required=True)
    run_spark = batch_subparsers.add_parser(
        "run-spark", help="process Kafka bronze JSONL into Spark bronze/silver Parquet"
    )
    run_spark.add_argument("--root", type=Path, help="override the platform repository root")
    run_spark.add_argument("--input", type=Path, help="raw Kafka bronze JSONL root")
    run_spark.add_argument("--bronze-output", type=Path, help="Spark bronze Parquet root")
    run_spark.add_argument("--silver-output", type=Path, help="Spark silver Parquet root")
    run_spark.add_argument("--report-output", type=Path, help="data-quality report root")
    run_spark.add_argument("--master", default="local[*]", help="Spark master URL")
    run_spark.add_argument("--run-id", help="stable run id; defaults to generated id")
    run_spark.add_argument("--json", action="store_true", help="print machine-readable JSON")
    return parser


def _print_status(settings: Settings, as_json: bool) -> None:
    payload = settings.as_dict()
    if as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return

    print(f"project: {payload['project_name']}")
    print(f"root: {payload['project_root']}")
    print(f"environment: {payload['environment']}")
    print(f"symbol: {payload['default_symbol']}")
    print(f"source_timeframe: {payload['source_timeframe']}")
    print(f"canonical_timeframe: {payload['canonical_timeframe']}")
    print(f"timezone: {payload['timezone']}")
    print(f"storage_backend: {payload['storage_backend']}")
    print(f"kafka_enabled: {str(payload['kafka_enabled']).lower()}")
    print(f"kafka_bootstrap_servers: {payload['kafka_bootstrap_servers']}")
    print(f"kafka_raw_topic: {payload['kafka_raw_topic']}")
    print(f"clickhouse_enabled: {str(payload['clickhouse_enabled']).lower()}")
    print("paths:")
    for name, path in payload["paths"].items():
        state = "present" if Path(path).exists() else "missing"
        print(f"  {name}: {path} ({state})")


def _default_historical_path(settings: Settings) -> Path:
    return settings.paths.root.parent / "Trading_system" / "data" / "vn30f1m-future_2.csv"


def _run_load_historical(args: argparse.Namespace) -> int:
    from vn30f1m_dataset.loaders import (
        LoadSummary,
        load_historical_csv,
        resample_ohlcv,
        write_ohlcv_parquet,
    )

    settings = Settings.from_env(args.root)
    input_path = (args.input or _default_historical_path(settings)).expanduser().resolve()
    source_timeframe = settings.source_timeframe
    target_timeframe = args.timeframe or settings.canonical_timeframe
    raw = load_historical_csv(
        input_path,
        symbol=args.symbol or settings.default_symbol,
        source_timezone=args.source_timezone or settings.timezone,
    )
    output = resample_ohlcv(raw, target_timeframe)
    output_root = (
        args.output
        or settings.paths.landing / "vn30f1m" / "ohlcv_intraday"
    ).expanduser().resolve()
    write_ohlcv_parquet(raw, output_root)
    if target_timeframe != source_timeframe:
        write_ohlcv_parquet(output, output_root)

    summary = LoadSummary(
        input_path=str(input_path),
        source_rows=len(raw),
        output_rows=len(output),
        source_timeframe=source_timeframe,
        output_timeframe=target_timeframe,
        min_event_time=output["event_time"].min().isoformat(),
        max_event_time=output["event_time"].max().isoformat(),
        output_path=str(output_root),
        ingest_run_id=str(raw["ingest_run_id"].iloc[0]),
    ).as_dict()
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(f"input: {summary['input_path']}")
        print(f"source rows: {summary['source_rows']} ({summary['source_timeframe']})")
        print(f"output rows: {summary['output_rows']} ({summary['output_timeframe']})")
        print(f"event time: {summary['min_event_time']} -> {summary['max_event_time']}")
        print(f"output: {summary['output_path']}")
        print(f"ingest_run_id: {summary['ingest_run_id']}")
    return 0


def _run_publish_csv(args: argparse.Namespace) -> int:
    from vn30f1m_streaming import KafkaOHLCVProducer

    settings = Settings.from_env(args.root)
    with KafkaOHLCVProducer.from_settings(settings) as producer:
        count = producer.send_csv(
            args.input,
            symbol=args.symbol or settings.default_symbol,
            source_timezone=args.source_timezone or settings.timezone,
            limit=args.limit,
        )
    summary = {"source": "historical_csv", "published": count, "topic": settings.kafka_raw_topic}
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(f"published: {count}")
        print(f"topic: {settings.kafka_raw_topic}")
    return 0


def _run_publish_dnse(args: argparse.Namespace) -> int:
    from vn30f1m_dataset import DNSEClient, DNSEClientConfig
    from vn30f1m_streaming import KafkaOHLCVProducer

    settings = Settings.from_env(args.root)
    with DNSEClient(DNSEClientConfig.from_settings(settings)) as client:
        frame = client.get_ohlcv_futures(
            symbol=args.symbol or settings.default_symbol,
            from_timestamp=args.from_timestamp,
            to_timestamp=args.to_timestamp,
            resolution=args.resolution,
        )
    with KafkaOHLCVProducer.from_settings(settings) as producer:
        count = producer.send_dataframe(frame, source="dnse_api")
    summary = {"source": "dnse_api", "fetched": len(frame), "published": count, "topic": settings.kafka_raw_topic}
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(f"fetched: {len(frame)}")
        print(f"published: {count}")
        print(f"topic: {settings.kafka_raw_topic}")
    return 0


def _run_consume_once(args: argparse.Namespace) -> int:
    from vn30f1m_streaming import KafkaOHLCVConsumer

    settings = Settings.from_env(args.root)
    kwargs = {}
    if args.output:
        kwargs["bronze_root"] = args.output.expanduser().resolve()
    with KafkaOHLCVConsumer.from_settings(settings, **kwargs) as consumer:
        summary = consumer.run_once(
            timeout_ms=args.timeout_ms,
            max_records=args.max_records,
        )
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        for name, count in summary.items():
            print(f"{name}: {count}")
    return 0


def _run_spark_batch(args: argparse.Namespace) -> int:
    from vn30f1m_batch import SparkBatchConfig, run_spark_batch_local

    settings = Settings.from_env(args.root)
    config = SparkBatchConfig.from_settings(
        settings,
        input_root=args.input,
        bronze_output=args.bronze_output,
        silver_output=args.silver_output,
        report_output=args.report_output,
        master=args.master,
        run_id=args.run_id,
    )
    summary = run_spark_batch_local(config).as_dict()
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(f"run_id: {summary['run_id']}")
        print(f"input rows: {summary['input_rows']}")
        print(f"valid rows: {summary['valid_rows']}")
        print(f"rejected rows: {summary['rejected_rows']}")
        print(f"duplicate rows: {summary['duplicate_rows']}")
        print(f"bronze: {summary['bronze_output']}")
        print(f"silver: {summary['silver_output']}")
        print(f"report: {summary['report_output']}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "status":
            settings = Settings.from_env(args.root)
            _print_status(settings, args.json)
            return 0
        if args.command == "dataset" and args.dataset_command == "load-historical":
            return _run_load_historical(args)
        if args.command == "stream" and args.stream_command == "publish-csv":
            return _run_publish_csv(args)
        if args.command == "stream" and args.stream_command == "publish-dnse":
            return _run_publish_dnse(args)
        if args.command == "stream" and args.stream_command == "consume-once":
            return _run_consume_once(args)
        if args.command == "batch" and args.batch_command == "run-spark":
            return _run_spark_batch(args)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    parser.error(f"unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
