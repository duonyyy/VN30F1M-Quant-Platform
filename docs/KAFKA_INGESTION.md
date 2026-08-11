# Kafka ingestion MVP

Phase 05 đưa Kafka vào đường ingest bắt buộc của MVP.

## Chạy broker local

Docker Compose dùng Apache Kafka KRaft một node cho môi trường phát triển:

```powershell
cd "C:\Users\Admin\Desktop\AI TOXIC\Nhom_1\vn30f1m_platform"
docker compose -f infra/docker-compose.yml up -d
```

Compose tạo hai topic:

- `vn30f1m.ohlcv.raw`: raw OHLCV event.
- `vn30f1m.ohlcv.raw.dlq`: record không hợp lệ.

Ứng dụng chạy trên máy host dùng `localhost:9092`; container nội bộ dùng
`kafka:29092`.

## Cài streaming extra

```powershell
python -m pip install -e ".[streaming]"
```

## Publish CSV

```powershell
python -m vn30f1m_core.cli stream publish-csv `
  --input "..\Trading_system\data\vn30f1m-future_2.csv" `
  --limit 100 `
  --json
```

Producer dùng message key:

```text
symbol|event_time_utc|timeframe
```

Ví dụ: `VN30F1M|2026-08-06T02:15:00Z|1m`.

## Consume vào bronze

```powershell
python -m vn30f1m_core.cli stream consume-once --max-records 100 --json
```

Output mặc định:

```text
lakehouse/bronze/vn30f1m/ohlcv_raw/
  symbol=VN30F1M/timeframe=1m/trading_date=YYYY-MM-DD/events.jsonl
```

Consumer kiểm tra schema, business key, UTC timestamp, OHLC bounds và volume.
Record lỗi đi vào DLQ; record trùng business key được bỏ qua. SQLite state nằm
trong `_consumer_state` để replay Kafka không ghi trùng bronze.

## DNSE realtime adapter

Khi có credential và endpoint hoạt động, có thể nối DNSE client với producer:

```powershell
python -m vn30f1m_core.cli stream publish-dnse `
  --symbol VN30F1M `
  --resolution 1 `
  --json
```

Phase này mới hoàn thành ingestion/replay. Nó chưa resample thành `15m`, chưa
feature engineering và chưa dự đoán model realtime.

Mô hình local một node/KRaft này phù hợp development; không nên coi là cấu
hình production cluster.
