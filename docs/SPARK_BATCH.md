# Spark batch pipeline

Phase 05B xử lý các event JSONL do Kafka consumer ghi ra:

```text
Kafka bronze JSONL
        |
        v
Spark read + schema + quality rules
        |
        +--> bronze Parquet: giữ cả valid/rejected/duplicate
        |
        +--> silver Parquet: chỉ valid và giữ bản ghi mới nhất theo business key
        |
        +--> data-quality report: Parquet + JSON
```

## Cài Spark

```powershell
python -m pip install -e ".[bigdata]"
```

## Chạy local

Sau khi Phase 05 consumer đã ghi dữ liệu vào
`lakehouse/bronze/vn30f1m/ohlcv_raw`:

```powershell
python -m vn30f1m_core.cli batch run-spark --json
```

Có thể chỉ định đường dẫn:

```powershell
python -m vn30f1m_core.cli batch run-spark `
  --input "lakehouse\bronze\vn30f1m\ohlcv_raw" `
  --bronze-output "lakehouse\bronze\vn30f1m\ohlcv_intraday" `
  --silver-output "lakehouse\silver\vn30f1m\ohlcv_intraday" `
  --report-output "lakehouse\reports\data_quality\ohlcv_intraday" `
  --master "local[*]" `
  --json
```

## Quy tắc chất lượng

Spark đánh dấu `rejected` nếu schema, UTC timestamp, `trading_date`,
timeframe, giá OHLC, volume hoặc OHLC bounds sai. Bronze vẫn giữ record lỗi để
audit; Silver loại record lỗi. Duplicate được thống kê trong report và Silver
chỉ giữ record mới nhất theo `consumed_at`/Kafka offset.

Output mặc định:

- `lakehouse/bronze/vn30f1m/ohlcv_intraday`: normalized Spark Bronze Parquet.
- `lakehouse/silver/vn30f1m/ohlcv_intraday`: valid, deduplicated Silver Parquet.
- `lakehouse/reports/data_quality/ohlcv_intraday`: report Parquet, JSON và summary.

Phase này chưa tạo feature, label hoặc model. Các phần đó thuộc Phase 06–07.
