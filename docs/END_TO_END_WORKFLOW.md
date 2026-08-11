# End-to-End Workflow

## 1. MVP workflow

Quyết định Phase 01: Kafka nằm trong MVP ingestion path. Workflow vẫn phục vụ batch ML/backtest, nhưng dữ liệu đầu vào phải đi qua Kafka trước khi ghi lakehouse.

```text
1. Start infra
2. Load historical VN30F1M data hoặc DNSE data
3. Publish OHLCV raw events vào Kafka
4. Consume Kafka events vào landing/bronze
5. Spark silver validation
6. Feature engineering
7. Run strategies/backtest
8. Export reports/metrics
9. Open dashboard
```

Resample OHLCV dùng `first/max/min/last/sum` cho `open/high/low/close/volume`, theo từng symbol và phiên giao dịch; không forward-fill qua khoảng nghỉ phiên.

## 2. Lệnh mục tiêu

Sau khi cài package editable bằng `python -m pip install -e . --no-deps`:

```powershell
vn30f1m status
vn30f1m kafka up
vn30f1m dataset load-historical
vn30f1m dataset publish-kafka
vn30f1m dataset consume-kafka
vn30f1m spark build
vn30f1m analysis run-baseline
vn30f1m dashboard
vn30f1m pipeline batch
```

## 3. Input MVP

Nguồn mẫu ban đầu:

```text
Trading_system/data/vn30f1m-future_2.csv
Trading_system/data/expiration_date.csv
```

Sau khi migrate:

```text
Kafka topic: vn30f1m.ohlcv.raw
vn30f1m_platform/lakehouse/landing/vn30f1m/
```

## 4. Output MVP

```text
vn30f1m_platform/lakehouse/bronze/
vn30f1m_platform/lakehouse/silver/
vn30f1m_platform/lakehouse/gold/
vn30f1m_platform/lakehouse/reports/
```

## 5. Failure rules

| Lỗi | Hành động |
|---|---|
| Dữ liệu OHLCV rỗng | Dừng pipeline |
| Duplicate datetime | Fail hoặc deduplicate theo rule rõ |
| Feature bị leakage | Fail test |
| Backtest không có lệnh | Cho pass nhưng report rõ |
| ClickHouse lỗi | Dùng Parquet fallback |
| Kafka publish/consume lỗi | Dừng pipeline MVP vì Kafka nằm trong ingestion path |
