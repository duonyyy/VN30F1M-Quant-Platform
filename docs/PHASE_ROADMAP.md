# VN30F1M Phase Roadmap

Roadmap này là ưu tiên triển khai hiện tại.

## Phase 00 - Khóa mục tiêu VN30F1M

Mục tiêu: xác nhận project này phục vụ VN30F1M/futures, không phải bank stocks.

Tasks:

- [x] Tạo `vn30f1m_platform/`.
- [x] Tách khỏi `bank_stock_platform/`.
- [x] Ghi rõ dùng open-source infra.
- [x] Ghi rõ `Trading_system` là source migration.

Output:

- `README.md`.
- `docs/PROJECT_OVERVIEW.md`.

Acceptance:

- Người đọc phân biệt được VN30F1M project và bank project.

## Phase 01 - Kiến trúc và contract

Mục tiêu: có docs nền trước khi migrate code.

Tasks:

- [x] Viết architecture overview.
- [x] Viết infra open-source plan.
- [x] Viết data contracts.
- [x] Viết mapping từ `Trading_system`.
- [x] Chốt timeframe MVP: `15m`; giữ cadence nguồn, ưu tiên raw `1m`.
- [x] Chốt Kafka nằm trong MVP ingestion path.

Output:

- Bộ docs trong `vn30f1m_platform/docs`.

Acceptance:

- Biết module nào đọc/ghi dữ liệu gì.
- Có một business key thống nhất: `symbol + event_time + timeframe`.
- Timestamp canonical là timezone-aware UTC; `trading_date` theo `Asia/Ho_Chi_Minh`.
- `15m` là timeframe duy nhất dùng cho ML/backtest MVP.
- Kafka nằm trong critical path MVP cho ingestion; batch có thể rebuild từ Parquet/MinIO sau khi dữ liệu đã được consume.
- Có rule resample, deduplication, nullability, provenance và chống leakage.

## Phase 02 - Core package và config

Mục tiêu: có config/path/CLI nền.

Tasks:

- [x] Tạo `vn30f1m_core/paths.py`.
- [x] Tạo `vn30f1m_core/settings.py`.
- [x] Tạo `vn30f1m_core/cli.py`.
- [x] Thêm command `status`.
- [x] Thêm `.env.example`.

Output:

- CLI skeleton.

Acceptance:

- `python -m vn30f1m_core.cli status` chạy được sau khi cài package editable.
- `vn30f1m status` chạy được qua console script.
- Settings đọc được biến môi trường và `.env`, với biến môi trường có precedence cao hơn.
- `status` không tự tạo thư mục runtime hoặc thay đổi dữ liệu.

## Phase 03 - Migrate historical loader

Mục tiêu: đọc được dữ liệu lịch sử từ `Trading_system/data/vn30f1m-future_2.csv`.

Tasks:

- [x] Tạo `vn30f1m_dataset/loaders.py`.
- [x] Migrate logic CSV loader từ `Trading_system/src/data/data_loader.py`.
- [x] Chuẩn hóa schema `ohlcv_intraday`.
- [x] Hỗ trợ resample timeframe.
- [x] Ghi output Parquet vào lakehouse landing.

Output:

- `lakehouse/landing/vn30f1m/ohlcv_intraday`.

Acceptance:

- Load và resample dữ liệu mẫu thành công.
- CSV legacy được diễn giải theo `Asia/Ho_Chi_Minh` và lưu `event_time` theo UTC.
- Dữ liệu nguồn giữ ở `1m`; output canonical được sinh ở `15m`.
- Output Parquet nằm tại `lakehouse/landing/vn30f1m/ohlcv_intraday` và partition theo symbol/timeframe/trading_date.
- Duplicate business key, OHLC bất hợp lệ và số âm bị từ chối.

## Phase 04 - Migrate DNSE client

Mục tiêu: có client lấy dữ liệu VN30F1M từ DNSE.

Tasks:

- [x] Tạo `vn30f1m_dataset/sources/dnse_client.py`.
- [x] Tham khảo `Trading_system/src/data/dnse_client.py`.
- [x] Đối chiếu phạm vi market-data/auth với docs chính thức DNSE; endpoint và version vẫn cấu hình được.
- [x] Thêm retry/timeout/logging.
- [x] Không hardcode token.

Output:

- DNSE client mới.

Acceptance:

- Có thể mock endpoint và chạy thử bằng credential/env nếu DNSE account được cấp.

## Phase 05 - Kafka ingestion MVP

Mục tiêu: đưa Kafka vào đường ingest bắt buộc của MVP.

Tasks:

- [x] Tạo Docker Compose service cho Kafka.
- [x] Chốt topic `vn30f1m.ohlcv.raw`.
- [x] Viết producer publish CSV/DNSE OHLCV vào Kafka.
- [x] Viết consumer ghi Kafka events vào landing/bronze.
- [x] Thêm message key `symbol + event_time + timeframe`.
- [x] Thêm dead-letter topic cho record lỗi.

Output:

- Kafka topic raw OHLCV.
- Landing/bronze được ghi từ Kafka consumer.

Acceptance:

- Publish/consume được dữ liệu mẫu bằng CLI hoặc mock test.
- Duplicate event xử lý theo business key.

## Phase 05B - Batch pipeline bằng Spark

Mục tiêu: Spark xử lý dữ liệu intraday theo batch.

Tasks:

- [x] Tạo Spark common config.
- [x] Tạo bronze ingest.
- [x] Tạo silver validation.
- [x] Ghi bronze/silver Parquet.
- [x] Sinh data quality report.

Output:

- `lakehouse/bronze`.
- `lakehouse/silver`.
- `lakehouse/reports/data_quality`.

Acceptance:

- Spark job có CLI local và test contract; chạy thật cần cài PySpark và có dữ liệu Bronze.

## Phase 06 - ML feature engineering

Mục tiêu: migrate feature kỹ thuật và feature ML từ `Trading_system/scripts/create_ml_alpha.py`.

Tasks:

- [x] Tạo `vn30f1m_analysis/ml/features.py`.
- [x] Migrate return, momentum, volatility, range features.
- [x] Migrate ATR, RSI, Bollinger Bands, EMA, ADX.
- [x] Migrate linear regression + ATR z-score features.
- [x] Migrate volume/session features.
- [x] Shift toàn bộ predictors một bar để chống leakage.
- [x] Thêm test chống leakage.
- [x] Ghi gold features.

Output:

- `lakehouse/gold/vn30f1m_features`.

Acceptance:

- Feature tính theo thứ tự thời gian, không dùng future data.
- Predictor tại `t` chỉ dùng dữ liệu biết đến `t-1`.
- Có version `vn30f1m_features_v1` và phân biệt row `warmup`/`ready`.

## Phase 06B - ML label, split và candidate baseline

Mục tiêu: xây dựng lõi ML trước khi làm dashboard.

Tasks:

- [ ] Tạo `vn30f1m_analysis/ml/labels.py`.
- [ ] Dùng forward return: `Close.shift(-horizon) / Close - 1`.
- [ ] Tạo label Long/Short với vùng nhiễu bị loại.
- [ ] Tạo split train/validation/test theo thời gian.
- [ ] Tạo feature selection native Spark (`ChiSqSelector`) trên train.
- [ ] Dựng Spark MLlib Pipeline với `VectorAssembler` và estimator Spark native.
- [ ] Tạo baseline `logistic_h4` timeframe `15m`.
- [ ] Chọn signal policy trên validation, không dùng test.

Output:

- ML dataset train/valid/test.
- Baseline Logistic Regression.

Acceptance:

- Không random split.
- Không fit scaler/imputer/selector trên validation/test.
- Có holdout test report riêng.

## Phase 07 - Migrate backtest engine

Mục tiêu: có backtest futures local.

Tasks:

- [ ] Tạo `vn30f1m_analysis/backtesting.py`.
- [ ] Migrate PnL futures.
- [ ] Migrate Sharpe, MDD, Margin, Hit Rate.
- [ ] Tách plotting khỏi metric calculation.
- [ ] Thêm tests cho position {-1, 0, 1}.

Output:

- Backtest engine sạch hơn bản cũ.

Acceptance:

- Chạy được backtest trên dữ liệu mẫu.

## Phase 08 - Rule-based strategies

Mục tiêu: có baseline chiến lược trước ML.

Tasks:

- [ ] Tạo `strategies/`.
- [ ] Migrate Bollinger Bands strategy.
- [ ] Migrate ATR strategy.
- [ ] Chuẩn hóa output `vn30f1m_signals`.
- [ ] Sinh report baseline.

Output:

- Baseline signals và metrics.

Acceptance:

- Có ít nhất 2 strategy rule-based chạy được.

## Phase 09 - Dashboard local

Mục tiêu: có Streamlit dashboard không phụ thuộc BigQuery/GCS.

Tasks:

- [ ] Tạo `apps/futures_dashboard/app.py`.
- [ ] Refactor từ `Trading_system/src/trading/dashboard.py`.
- [ ] Đọc Parquet/ClickHouse local.
- [ ] Hiển thị PnL, signal, metrics, trade logs.
- [ ] Bỏ live BigQuery monitor khỏi MVP.

Output:

- Futures dashboard local.

Acceptance:

- Chạy dashboard được bằng dữ liệu local.

## Phase 10 - ClickHouse serving

Mục tiêu: có query layer nhanh.

Tasks:

- [ ] Tạo ClickHouse table `vn30f1m_features`.
- [ ] Tạo table `vn30f1m_signals`.
- [ ] Tạo table `backtest_metrics`.
- [ ] Load Parquet sang ClickHouse.
- [ ] Dashboard đọc được ClickHouse.

Output:

- Serving layer.

Acceptance:

- Query theo timeframe/date range được.

## Phase 11 - ML comparison and report

Mục tiêu: so sánh các model ML và sinh báo cáo chính thức.

Tasks:

- [ ] Migrate pipeline từ `Trading_system/scripts/create_ml_alpha.py`.
- [ ] Train/test split theo thời gian.
- [ ] Lưu model artifact local/MinIO.
- [ ] Sinh ML signal.
- [ ] Sinh ML report.
- [ ] So sánh Spark MLlib Logistic Regression, Random Forest và GBTClassifier.
- [ ] Re-benchmark ExtraTrees/XGBoost chỉ khi cần đối chiếu ngoài Spark MLlib.

Output:

- ML baseline report.

Acceptance:

- Không random split.
- Không dùng future label làm feature.
- Không chọn model theo test set.

## Phase 12 - Kafka monitoring and streaming hardening

Mục tiêu: tăng độ ổn định cho Kafka sau khi MVP ingest đã chạy.

Tasks:

- [x] Chốt Apache Kafka làm message broker streaming.
- [ ] Thêm consumer lag monitoring.
- [ ] Thêm retry/dead-letter handling.
- [ ] Thêm schema/version check cho Kafka messages.
- [ ] Ghi stream trực tiếp vào ClickHouse nếu cần realtime dashboard.
- [ ] Thêm lag/health monitoring.

Output:

- Kafka observability/hardening.

Acceptance:

- Kafka lỗi phải có log, retry hoặc dead-letter rõ ràng.

## Phase 13 - Testing

Mục tiêu: kiểm chứng dữ liệu và backtest.

Tasks:

- [ ] Unit test loader.
- [ ] Unit test feature engineering.
- [ ] Unit test backtest metrics.
- [ ] Integration test CSV -> Spark -> features -> backtest.
- [ ] Integration test CSV/DNSE -> Kafka -> landing/bronze.
- [ ] Integration test dashboard data source.

Output:

- Test suite.

Acceptance:

- Test chạy được không cần GCP.

## Phase 14 - Deploy local/server

Mục tiêu: chạy được project trên server bằng open-source stack.

Tasks:

- [ ] Docker Compose infra.
- [ ] Runbook local.
- [ ] Runbook server.
- [ ] Backup lakehouse/ClickHouse.
- [ ] Scheduler batch.

Output:

- Deployment docs.

Acceptance:

- Một server có thể chạy batch pipeline và dashboard.
