# VN30F1M Platform - Tổng quan dự án

## 1. Mục tiêu

`vn30f1m_platform` là project ưu tiên cho hướng VN30F1M/futures/intraday.

Mục tiêu chính:

- Thu thập dữ liệu VN30F1M từ DNSE hoặc dữ liệu lịch sử nội bộ.
- Xử lý dữ liệu intraday bằng hạ tầng open-source.
- Tạo feature kỹ thuật cho phái sinh.
- Backtest chiến lược Long/Short/Hold.
- Hiển thị kết quả qua dashboard.
- Migrate logic tốt từ `Trading_system/`, nhưng không giữ GCP làm kiến trúc chính.

## 2. Không thuộc phạm vi project này

- Dataset 51 cột của cổ phiếu ngân hàng.
- Phân tích báo cáo tài chính ngân hàng.
- ChatVNS cho BCTC ngân hàng.
- BigQuery/PubSub/Cloud Run làm production path chính.

Các phần đó thuộc project khác hoặc legacy/reference.

## 3. Kiến trúc định hướng

Quyết định của Phase 01: dữ liệu nguồn giữ cadence gốc, ưu tiên `1m`; `15m` là timeframe canonical cho ML/backtest MVP. Kafka là thành phần bắt buộc trong MVP ingestion path.

```text
DNSE / historical CSV
  -> vn30f1m_dataset adapter
  -> Kafka topic vn30f1m.ohlcv.raw
  -> landing / bronze
  -> Spark batch / bronze -> silver
  -> canonical OHLCV 15m
  -> Parquet / MinIO lakehouse
  -> ClickHouse serving
  -> vn30f1m_analysis
  -> futures_dashboard
```

## 4. Thành phần chính

| Thành phần | Vai trò |
|---|---|
| `vn30f1m_dataset` | Thu thập/load dữ liệu VN30F1M và publish raw OHLCV events vào Kafka |
| `pipelines/batch_jobs` | Spark batch pipeline cho dữ liệu lịch sử/intraday |
| `pipelines/streaming_jobs` | Kafka producer/consumer cho MVP ingestion |
| `vn30f1m_analysis` | Feature, strategy, backtest, report |
| `futures_dashboard` | Streamlit dashboard cho PnL, signal, metrics |
| `vn30f1m_core` | Config, path, CLI, orchestration |
| `legacy` | Code cũ từ `Trading_system` chưa migrate |

## 5. Nguyên tắc triển khai

- Dùng Kafka trong MVP để thay vai trò Pub/Sub và lưu event log ingestion.
- Dùng Spark batch cho ML/backtest MVP sau khi dữ liệu đã vào lakehouse.
- Dùng open-source stack: Apache Kafka, Spark, ClickHouse, MinIO/Parquet.
- Chỉ migrate code cũ khi đã rõ owner và output.
- Không để dashboard phụ thuộc BigQuery/GCS trong MVP.
- Không trộn project này với `bank_stock_platform`.
