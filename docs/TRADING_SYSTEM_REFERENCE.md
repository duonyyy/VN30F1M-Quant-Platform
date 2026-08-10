# Tham khảo từ `Trading_system`

`Trading_system/` là nguồn tham khảo chính cho project `vn30f1m_platform/`.

Tuy nhiên không nên copy toàn bộ một lần. Code cũ có cả local backtest, Streamlit dashboard, GCP Pub/Sub, BigQuery, Telegram và nhiều notebook thử nghiệm. Khi migrate cần tách theo lớp và thay hạ tầng cloud bằng open-source.

Nguyên tắc mới: chỉ tham khảo logic nghiệp vụ từ `Trading_system`, không giữ GCP làm kiến trúc chính.

## 1. Mapping đề xuất

| Trong `Trading_system` | Sang `vn30f1m_platform` | Ghi chú |
|---|---|---|
| `src/data/dnse_client.py` | `packages/vn30f1m_dataset/vn30f1m_dataset/sources/dnse_client.py` | Tham khảo cách gọi DNSE, cần kiểm tra lại endpoint theo docs chính thức |
| `src/data/data_loader.py` | `packages/vn30f1m_dataset/vn30f1m_dataset/loaders.py` | Dùng cho CSV/intraday/resample |
| `src/data/feature_engineering.py` | `packages/vn30f1m_analysis/vn30f1m_analysis/features.py` | Có ATR, RSI, BB, EMA, ADX, lag features |
| `src/trading/backtest.py` | `packages/vn30f1m_analysis/vn30f1m_analysis/backtesting.py` | Có PnL futures, Sharpe, MDD, Margin, Hit Rate |
| `src/trading/strategy/*` | `packages/vn30f1m_analysis/vn30f1m_analysis/strategies/` | Migrate từng strategy sau khi loader/backtest ổn |
| `src/models/*` | `packages/vn30f1m_analysis/vn30f1m_analysis/models/` | Chỉ migrate sau phase baseline |
| `src/trading/dashboard.py` | `apps/futures_dashboard/app.py` | Nên refactor bớt dependency GCP và path cũ |
| `configs/precomputed_backtests/` | `lakehouse/reports/backtests/` hoặc `artifacts/backtests/` | Không nên để config chứa data output lớn |
| `data/vn30f1m-future_2.csv` | `lakehouse/landing/vn30f1m/` | Dữ liệu mẫu/landing, không coi là source production duy nhất |
| `src/data/pubsub_publisher.py` | `pipelines/streaming_jobs/kafka_producer.py` | Tham khảo ý tưởng publisher, nhưng viết lại bằng Kafka |
| `src/data/bigquery_sink.py` | `legacy/gcp_bigquery/` | Thay bằng ClickHouse/Parquet/MinIO trong kiến trúc mới |
| `telegram_bot.py` | `legacy/telegram_alerts/` hoặc phase alert riêng | Không nên gắn vào lõi analysis |

## 2. Mapping hạ tầng sang open-source

| Trong `Trading_system` cũ | Open-source thay thế trong `vn30f1m_platform` |
|---|---|
| GCP Pub/Sub | Apache Kafka |
| BigQuery | ClickHouse |
| GCS model/backtest cache | MinIO hoặc local artifacts |
| Cloud Run publisher/subscriber | Docker Compose services |
| Cloud Scheduler | Cron, Airflow hoặc Prefect |
| Looker Studio | Streamlit dashboard |
| GCP logs | File logs + ClickHouse observability table |

## 3. Nên migrate trước

Ưu tiên:

1. CSV/intraday data loader.
2. Feature engineering.
3. Backtest engine.
4. Rule-based strategies: Bollinger Bands, ATR.
5. Precomputed backtest reader.
6. Futures dashboard local.

Lý do: đây là lõi nghiên cứu VN30F1M và có thể chạy local trước khi động tới cloud/streaming.

## 4. Nên để sau

Để phase sau:

- Telegram alert.
- Paper trading monitor.

Telegram và paper trading làm deployment nặng hơn. Kafka không để sau nữa vì đã thuộc MVP ingestion path.

## 5. Rủi ro khi bê nguyên `Trading_system`

| Rủi ro | Vì sao |
|---|---|
| Path cũ bị hardcode | Nhiều file đang trỏ `Trading_system/data/...` hoặc `project_root` kiểu cũ |
| GCP dependency làm fail local | Dashboard có nhánh đọc BigQuery/GCS |
| Dashboard quá lớn | File `dashboard.py` chứa cả UI, data loading, precomputed cache và live monitor |
| Code research lẫn production | Notebook, model artifact, reports và scripts chưa tách ownership |
| Dễ lẫn với bank project | Nếu giữ tên `Trading_system`, người đọc sẽ nhầm với `bank_stock_analysis` |

## 6. Phase riêng cho VN30F1M

```text
Phase 01: Migrate loader CSV/intraday
Phase 02: Migrate feature engineering
Phase 03: Migrate backtest engine
Phase 04: Migrate BB/ATR strategies
Phase 05: Migrate ML baseline
Phase 06: Build futures dashboard local
Phase 07: Add report/precomputed backtest output
Phase 08: Add ClickHouse/MinIO serving
Phase 09: Kafka ingestion path trong MVP
Phase 10: Optional Telegram/paper trading monitor
```

## 7. Kết luận

`Trading_system` rất hữu ích cho `vn30f1m_platform`, nhưng nên dùng như source migration, không dùng làm production structure mới.

Project bank stocks vẫn giữ riêng ở `bank_stock_platform/`.
