# Testing and Acceptance

## 0. Acceptance Phase 01 - Kiến trúc và contract

Phase 01 được coi là hoàn tất khi:

- `15m` được xác định là timeframe canonical cho ML/backtest MVP; raw cadence ưu tiên `1m`.
- Kafka là dependency bắt buộc của MVP ingestion path.
- Mọi bảng canonical dùng `snake_case`, `event_time` UTC và `trading_date` theo `Asia/Ho_Chi_Minh`.
- Business key `symbol + event_time + timeframe` được dùng nhất quán cho dữ liệu theo bar.
- Có ownership/read-write matrix cho dataset, batch, analysis, serving và dashboard.
- Contract nêu rõ schema, nullability, allowed values, deduplication, provenance, resample và leakage rule.
- Có thể rebuild serving/report từ lakehouse mà không cần GCP; Kafka dùng để replay/audit ingestion.

## 1. Mục tiêu kiểm thử

Kiểm thử phải chứng minh:

1. Không cần GCP vẫn chạy được MVP.
2. Dữ liệu VN30F1M được load/resample đúng.
3. Feature không bị leakage.
4. Backtest tính PnL/metrics đúng.
5. Dashboard đọc dữ liệu local/ClickHouse được.

## 2. Unit tests

- Loader đọc CSV.
- Resample timeframe.
- OHLCV schema.
- ATR, RSI, BB, EMA, ADX.
- Label horizon.
- Backtest position {-1, 0, 1}.
- Metrics Sharpe, MDD, Margin, Hit Rate.

## 3. Integration tests

- CSV -> landing Parquet.
- CSV/DNSE -> Kafka topic.
- Kafka topic -> landing/bronze.
- Landing -> Spark bronze/silver.
- Silver -> features.
- Features -> strategy signals.
- Signals -> backtest metrics.
- Reports -> dashboard.

## 4. Acceptance MVP

Pass khi:

- Chạy được batch pipeline không cần GCP.
- Kafka publish/consume chạy được trong Docker Compose.
- Có gold features.
- Có ít nhất 2 strategy baseline.
- Có backtest metrics.
- Dashboard mở được.
- Docs nói rõ GCP chỉ là legacy/reference.

Fail khi:

- Dashboard bắt buộc BigQuery/GCS mới chạy.
- Loader hardcode path `Trading_system`.
- Feature dùng dữ liệu tương lai.
- Không có test cho backtest.
