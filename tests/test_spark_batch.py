from __future__ import annotations

import importlib.util

import pytest

from vn30f1m_batch import SparkBatchConfig, SparkBatchError, run_spark_batch_local
from vn30f1m_core.settings import Settings


def test_spark_batch_config_uses_platform_paths():
    settings = Settings.from_env()
    config = SparkBatchConfig.from_settings(settings, run_id="spark_test")

    assert config.run_id == "spark_test"
    assert config.input_root.name == "ohlcv_raw"
    assert config.bronze_output.name == "ohlcv_intraday"
    assert config.silver_output.parts[-2:] == ("vn30f1m", "ohlcv_intraday")
    assert config.as_dict()["master"] == "local[*]"


@pytest.mark.skipif(importlib.util.find_spec("pyspark") is not None, reason="PySpark is installed")
def test_spark_batch_reports_missing_optional_dependency():
    config = SparkBatchConfig(
        input_root="lakehouse/bronze/vn30f1m/ohlcv_raw",
        bronze_output="lakehouse/bronze/vn30f1m/ohlcv_intraday",
        silver_output="lakehouse/silver/vn30f1m/ohlcv_intraday",
        report_output="lakehouse/reports/data_quality/ohlcv_intraday",
    )

    with pytest.raises(SparkBatchError, match="PySpark is required"):
        run_spark_batch_local(config)
