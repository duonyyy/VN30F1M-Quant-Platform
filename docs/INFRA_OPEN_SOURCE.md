# Open-Source Infrastructure Plan

## 1. Mục tiêu infra

Hạ tầng chính của `vn30f1m_platform` phải dùng open-source, dễ chạy local/server bằng Docker Compose.

MVP không dùng GCP làm production path chính.

## 2. Stack đề xuất

**Quyết định:** sử dụng Apache Kafka làm message broker streaming chính của project. Các broker thay thế không thuộc kiến trúc này.

| Nhu cầu | Open-source stack |
|---|---|
| Batch processing | Apache Spark |
| Streaming message broker | Apache Kafka |
| Lakehouse storage | Parquet local hoặc MinIO |
| Serving/query | ClickHouse |
| Dashboard | Streamlit |
| Orchestration | CLI trước, sau đó Airflow/Prefect/Cron |
| Observability | Logs + ClickHouse observability tables |

## 3. MVP infra tối thiểu

MVP đầu tiên cần:

```text
Kafka
Spark
ClickHouse
Parquet local hoặc MinIO
Streamlit
```

Kafka là thành phần bắt buộc trong MVP ingestion path. MVP chưa cần realtime trading, nhưng vẫn phải có Kafka để chuẩn hóa event log, replay dữ liệu và thay thế vai trò Pub/Sub trong kiến trúc cũ.

## 4. Port đề xuất

| Service | Port |
|---|---:|
| Spark master UI | 8080 |
| Spark master | 7077 |
| ClickHouse HTTP | 8123 |
| ClickHouse native | 9002 |
| MinIO API | 9000 |
| MinIO console | 9001 |
| Kafka broker | 9092 |
| Kafka UI (tuỳ chọn) | 8082 |
| Streamlit dashboard | 8503 |

## 5. Quan hệ với `Trading_system`

| `Trading_system` cũ | Infra mới |
|---|---|
| Pub/Sub | Apache Kafka |
| BigQuery | ClickHouse |
| GCS | MinIO/local artifacts |
| Cloud Run | Docker Compose services |
| Cloud Scheduler | Cron/Airflow/Prefect |

## 6. Nguyên tắc

- ClickHouse là serving layer, không phải source of truth.
- Source of truth là Parquet/MinIO lakehouse.
- Kafka là bắt buộc cho ingestion MVP.
- Spark/ML/backtest vẫn có thể chạy theo batch từ dữ liệu đã consume ra lakehouse.
- Realtime trading chưa thuộc MVP; Kafka trong MVP phục vụ ingestion, replay và audit.
