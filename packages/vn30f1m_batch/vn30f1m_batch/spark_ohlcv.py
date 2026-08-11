"""Spark batch pipeline for raw Kafka bronze events.

The job reads JSONL emitted by the Phase 05 Kafka consumer, preserves every
event in a bronze Parquet dataset, and writes only valid/latest business-key
rows to silver. PySpark is imported lazily so non-Spark commands remain usable.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

LOGGER = logging.getLogger(__name__)
DEFAULT_TIMEZONE = "Asia/Ho_Chi_Minh"
DEFAULT_MASTER = "local[*]"


class SparkBatchError(RuntimeError):
    """Raised when the Spark batch cannot be configured or completed."""


@dataclass(frozen=True, slots=True)
class SparkBatchConfig:
    """Paths and runtime settings for one reproducible Spark batch run."""

    input_root: Path
    bronze_output: Path
    silver_output: Path
    report_output: Path
    timezone: str = DEFAULT_TIMEZONE
    master: str = DEFAULT_MASTER
    run_id: str = ""

    def __post_init__(self) -> None:
        for name in ("input_root", "bronze_output", "silver_output", "report_output"):
            value = getattr(self, name)
            if not isinstance(value, Path):
                object.__setattr__(self, name, Path(value))
        if not self.timezone.strip():
            raise ValueError("timezone must not be empty")
        if not self.master.strip():
            raise ValueError("master must not be empty")

    @classmethod
    def from_settings(
        cls,
        settings: Any,
        *,
        input_root: str | Path | None = None,
        bronze_output: str | Path | None = None,
        silver_output: str | Path | None = None,
        report_output: str | Path | None = None,
        master: str = DEFAULT_MASTER,
        run_id: str | None = None,
    ) -> "SparkBatchConfig":
        paths = settings.paths
        return cls(
            input_root=Path(input_root or paths.bronze / "vn30f1m" / "ohlcv_raw"),
            bronze_output=Path(
                bronze_output or paths.bronze / "vn30f1m" / "ohlcv_intraday"
            ),
            silver_output=Path(
                silver_output or paths.silver / "vn30f1m" / "ohlcv_intraday"
            ),
            report_output=Path(
                report_output or paths.reports / "data_quality" / "ohlcv_intraday"
            ),
            timezone=settings.timezone,
            master=master,
            run_id=run_id or f"spark_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}_{uuid4().hex[:8]}",
        )

    def as_dict(self) -> dict[str, object]:
        values = asdict(self)
        for key in ("input_root", "bronze_output", "silver_output", "report_output"):
            values[key] = str(values[key])
        return values


@dataclass(frozen=True, slots=True)
class SparkBatchSummary:
    """Serializable result of a Spark batch run."""

    run_id: str
    input_rows: int
    distinct_business_keys: int
    duplicate_rows: int
    valid_rows: int
    rejected_rows: int
    bronze_output: str
    silver_output: str
    report_output: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _spark_modules() -> tuple[Any, Any, Any, Any]:
    try:
        from pyspark.sql import SparkSession, functions as F, types as T
        from pyspark.sql.window import Window
    except ImportError as exc:
        raise SparkBatchError(
            "PySpark is required; install it with python -m pip install -e .[bigdata]"
        ) from exc
    return SparkSession, Window, F, T


def create_spark_session(config: SparkBatchConfig) -> Any:
    """Create a Spark session with UTC event-time semantics."""

    SparkSession, _, _, _ = _spark_modules()
    builder = (
        SparkSession.builder.appName("VN30F1M-Spark-Batch")
        .master(config.master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    return builder.getOrCreate()


def _raw_schema(T: Any) -> Any:
    return T.StructType(
        [
            T.StructField("schema_version", T.StringType(), True),
            T.StructField("symbol", T.StringType(), True),
            T.StructField("event_time", T.StringType(), True),
            T.StructField("trading_date", T.StringType(), True),
            T.StructField("timeframe", T.StringType(), True),
            T.StructField("open", T.DoubleType(), True),
            T.StructField("high", T.DoubleType(), True),
            T.StructField("low", T.DoubleType(), True),
            T.StructField("close", T.DoubleType(), True),
            T.StructField("volume", T.DoubleType(), True),
            T.StructField("source", T.StringType(), True),
            T.StructField("source_record_id", T.StringType(), True),
            T.StructField("ingest_run_id", T.StringType(), True),
            T.StructField("published_at", T.StringType(), True),
            T.StructField("kafka_key", T.StringType(), True),
            T.StructField("kafka_partition", T.IntegerType(), True),
            T.StructField("kafka_offset", T.LongType(), True),
            T.StructField("consumed_at", T.StringType(), True),
            T.StructField("_corrupt_record", T.StringType(), True),
        ]
    )


def _read_raw_events(spark: Any, config: SparkBatchConfig, F: Any, T: Any) -> Any:
    if not config.input_root.exists():
        raise SparkBatchError(f"Spark input root not found: {config.input_root}")
    try:
        return (
            spark.read.option("mode", "PERMISSIVE")
            .option("recursiveFileLookup", "true")
            .schema(_raw_schema(T))
            .json(str(config.input_root))
        )
    except Exception as exc:
        raise SparkBatchError(f"Unable to read Spark input: {exc}") from exc


def _add_quality_columns(raw: Any, config: SparkBatchConfig, Window: Any, F: Any) -> Any:
    event_ts = F.to_timestamp(F.col("event_time"))
    published_ts = F.to_timestamp(F.col("published_at"))
    consumed_ts = F.to_timestamp(F.col("consumed_at"))
    local_date = F.date_format(
        F.from_utc_timestamp(event_ts, config.timezone), "yyyy-MM-dd"
    )
    numeric_present = F.col("open").isNotNull()
    for column in ("high", "low", "close", "volume"):
        numeric_present = numeric_present & F.col(column).isNotNull()
    valid = (
        F.col("_corrupt_record").isNull()
        & F.col("schema_version").eqNullSafe("ohlcv_raw_v1")
        & F.col("symbol").isNotNull()
        & (F.length(F.trim(F.col("symbol"))) > 0)
        & event_ts.isNotNull()
        & published_ts.isNotNull()
        & F.col("trading_date").eqNullSafe(local_date)
        & F.col("timeframe").isin("1m", "5m", "15m", "30m")
        & numeric_present
        & (F.col("open") > 0)
        & (F.col("high") > 0)
        & (F.col("low") > 0)
        & (F.col("close") > 0)
        & (F.col("volume") >= 0)
        & (F.col("high") >= F.greatest("open", "close", "low"))
        & (F.col("low") <= F.least("open", "close", "high"))
        & F.col("source").isNotNull()
        & F.col("ingest_run_id").isNotNull()
    )
    key_window = Window.partitionBy("symbol", "event_ts", "timeframe")
    rank_window = key_window.orderBy(
        F.col("consumed_ts").desc_nulls_last(),
        F.col("kafka_offset").desc_nulls_last(),
    )
    return (
        raw.withColumn("event_ts", event_ts)
        .withColumn("published_ts", published_ts)
        .withColumn("consumed_ts", consumed_ts)
        .withColumn("_is_valid", valid)
        .withColumn("duplicate_count", F.count(F.lit(1)).over(key_window))
        .withColumn("dedupe_rank", F.row_number().over(rank_window))
        .withColumn(
            "quality_status",
            F.when(F.col("_is_valid"), F.lit("valid")).otherwise(F.lit("rejected")),
        )
        .withColumn("is_duplicate", F.col("duplicate_count") > 1)
        .withColumn("event_time", F.col("event_ts"))
        .withColumn("published_at", F.col("published_ts"))
        .withColumn("consumed_at", F.col("consumed_ts"))
        .withColumn("trading_date", F.to_date(F.col("trading_date")))
    )


def _output_columns(frame: Any) -> list[Any]:
    return [
        frame[name]
        for name in (
            "schema_version", "symbol", "event_time", "trading_date", "timeframe",
            "open", "high", "low", "close", "volume", "source", "source_record_id",
            "ingest_run_id", "published_at", "consumed_at", "kafka_key", "kafka_partition",
            "kafka_offset", "quality_status", "is_duplicate", "duplicate_count",
        )
    ]


def _write_parquet(frame: Any, output_root: Path) -> None:
    output_root.parent.mkdir(parents=True, exist_ok=True)
    (
        frame.write.mode("overwrite")
        .partitionBy("symbol", "timeframe", "trading_date")
        .parquet(str(output_root))
    )


def _write_quality_report(spark: Any, report: dict[str, Any], output_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    report_frame = spark.createDataFrame([report])
    report_frame.write.mode("overwrite").parquet(str(output_root / "parquet"))
    report_frame.write.mode("overwrite").json(str(output_root / "json"))
    (output_root / "summary.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )


def run_spark_batch(spark: Any, config: SparkBatchConfig) -> SparkBatchSummary:
    """Execute raw JSONL -> bronze Parquet -> validated silver Parquet."""

    _, Window, F, T = _spark_modules()
    raw = _read_raw_events(spark, config, F, T)
    input_rows = raw.count()
    if input_rows == 0:
        raise SparkBatchError(f"Spark input contains no events: {config.input_root}")

    enriched = _add_quality_columns(raw, config, Window, F).cache()
    bronze = enriched.select(*_output_columns(enriched))
    silver = enriched.filter(
        (F.col("quality_status") == "valid") & (F.col("dedupe_rank") == 1)
    ).select(*_output_columns(enriched))
    _write_parquet(bronze, config.bronze_output)
    _write_parquet(silver, config.silver_output)

    metrics_row = (
        enriched.agg(
            F.count(F.lit(1)).alias("input_rows"),
            F.countDistinct("symbol", "event_time", "timeframe").alias(
                "distinct_business_keys"
            ),
            F.sum(F.when(F.col("is_duplicate"), 1).otherwise(0)).alias("duplicate_rows"),
            F.sum(F.when(F.col("quality_status") == "valid", 1).otherwise(0)).alias(
                "valid_rows"
            ),
            F.sum(F.when(F.col("quality_status") == "rejected", 1).otherwise(0)).alias(
                "rejected_rows"
            ),
        )
        .first()
        .asDict()
    )
    report = {
        "run_id": config.run_id,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "input_root": str(config.input_root),
        **{key: int(value or 0) for key, value in metrics_row.items()},
    }
    _write_quality_report(spark, report, config.report_output)
    enriched.unpersist()
    return SparkBatchSummary(
        run_id=config.run_id,
        input_rows=report["input_rows"],
        distinct_business_keys=report["distinct_business_keys"],
        duplicate_rows=report["duplicate_rows"],
        valid_rows=report["valid_rows"],
        rejected_rows=report["rejected_rows"],
        bronze_output=str(config.bronze_output),
        silver_output=str(config.silver_output),
        report_output=str(config.report_output),
    )


def run_spark_batch_local(config: SparkBatchConfig) -> SparkBatchSummary:
    """Create and stop a local Spark session around one batch run."""

    spark = create_spark_session(config)
    try:
        return run_spark_batch(spark, config)
    finally:
        spark.stop()
