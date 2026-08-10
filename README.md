# VN30F1M Futures Analytics Platform

Đây là project riêng cho hướng VN30F1M, tách khỏi `bank_stock_platform/`.

Mục tiêu:

- Thu thập dữ liệu VN30F1M/intraday.
- Phân tích phái sinh VN30F1M.
- Backtest chiến lược nghiên cứu.
- Xây dựng dashboard riêng cho futures.
- Dùng hạ tầng open-source thay cho GCP trong hướng triển khai chính.
- Giữ ranh giới rõ với dự án cổ phiếu ngân hàng.

Không dùng project này để phân tích dataset 51 cột của nhóm ngân hàng. Phần đó thuộc `bank_stock_platform/`.

Open-source stack định hướng:

```text
DNSE / CSV -> Kafka -> Spark batch/consumer -> Parquet/MinIO -> ClickHouse -> Streamlit
```

## Cấu trúc

```text
vn30f1m_platform/
  apps/
    futures_dashboard/
  packages/
    vn30f1m_dataset/
    vn30f1m_analysis/
    vn30f1m_core/
  pipelines/
    streaming_jobs/
    batch_jobs/
  infra/
  lakehouse/
  docs/
  legacy/
```

## Ranh giới với bank project

| Nội dung | Project xử lý |
|---|---|
| Cổ phiếu ngân hàng Việt Nam | `bank_stock_platform/` |
| Dataset 51 cột bank stocks | `bank_stock_platform/` |
| BCTC PDF/HTML ngân hàng cho ChatVNS | `bank_stock_platform/` |
| VN30F1M intraday/futures | `vn30f1m_platform/` |
| Backtest phái sinh VN30F1M | `vn30f1m_platform/` |

## Tài liệu tham khảo migration

- `docs/README.md`: thứ tự đọc bộ docs.
- `docs/PROJECT_OVERVIEW.md`: tổng quan canonical cho project VN30F1M.
- `docs/ARCHITECTURE_OVERVIEW.md`: sơ đồ tổng quan VN30F1M.
- `docs/INFRA_OPEN_SOURCE.md`: hạ tầng open-source thay GCP.
- `docs/DATA_CONTRACTS.md`: contract dữ liệu VN30F1M.
- `docs/ML_ALGORITHM_PLAN.md`: kế hoạch thuật toán ML, phần quan trọng nhất của project.
- `docs/PHASE_ROADMAP.md`: phase triển khai ưu tiên.
- `docs/END_TO_END_WORKFLOW.md`: workflow batch end-to-end.
- `docs/TESTING_AND_ACCEPTANCE.md`: kiểm thử và nghiệm thu.
- `docs/TRADING_SYSTEM_REFERENCE.md`: mapping từ `Trading_system/` sang project mới.

## CI

GitHub Actions workflow:

```text
.github/workflows/vn30f1m-ci.yml
```

CI hiện kiểm tra docs bắt buộc, cú pháp Python và tự chạy `pytest` khi thư mục `vn30f1m_platform/tests` được thêm vào.
