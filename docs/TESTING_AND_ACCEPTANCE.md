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

Phase 03 bổ sung acceptance:

- Loader đọc được CSV legacy `vn30f1m-future_2.csv`.
- Timestamp naive được localize theo `Asia/Ho_Chi_Minh` rồi chuyển UTC.
- Resample dùng `first/max/min/last/sum`, không forward-fill gap.
- Parquet có `0` duplicate business key và đạt validation OHLCV.

Phase 05 bổ sung acceptance:

- Docker Compose định nghĩa Kafka KRaft local và tạo `vn30f1m.ohlcv.raw` cùng DLQ.
- Producer publish được CSV/DNSE OHLCV với key `symbol|event_time|timeframe`.
- Consumer validate, ghi bronze JSONL và chống duplicate bằng business key.
- Record lỗi được route sang `vn30f1m.ohlcv.raw.dlq`.
- Unit/mock suite hiện chạy được `17 passed`; integration Docker cần Docker daemon đang chạy.

Phase 05B bổ sung acceptance:

- Spark đọc Bronze JSONL theo schema cố định và giữ record lỗi trong Bronze.
- Silver Parquet chỉ chứa record valid, deduplicate theo business key.
- Có report Parquet/JSON với input, valid, rejected và duplicate counts.
- CLI `vn30f1m batch run-spark` dùng được sau khi cài `.[bigdata]`.

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
