# VN30F1M Docs Index

Thứ tự đọc đề xuất:

1. `PROJECT_OVERVIEW.md` - mục tiêu và phạm vi project.
2. `ARCHITECTURE_OVERVIEW.md` - sơ đồ tổng quan kiến trúc.
3. `INFRA_OPEN_SOURCE.md` - hạ tầng open-source thay GCP.
4. `DATA_CONTRACTS.md` - schema và contract dữ liệu.
5. `ML_ALGORITHM_PLAN.md` - trọng tâm thuật toán ML, label, feature, split, model và nghiệm thu.
6. `TRADING_SYSTEM_REFERENCE.md` - mapping từ `Trading_system/`.
7. `PHASE_ROADMAP.md` - phase triển khai ưu tiên.
8. `END_TO_END_WORKFLOW.md` - workflow MVP có Kafka ingestion và batch ML/backtest.
9. `TESTING_AND_ACCEPTANCE.md` - kiểm thử và nghiệm thu.

Phase 01 đã chốt: canonical timeframe `15m`, event-time UTC, Kafka nằm trong MVP ingestion path, Parquet/MinIO là lakehouse và contract dùng business key `symbol + event_time + timeframe`.

Ghi nhớ: `vn30f1m_platform` là hướng ưu tiên hiện tại. `Trading_system` chỉ là nguồn tham khảo/migration, không phải cấu trúc production mới.
