# DNSE client

Phase 04 cung cấp adapter `vn30f1m_dataset.sources.DNSEClient` để lấy OHLCV
futures và trả về DataFrame đã chuẩn hóa. Adapter chưa publish Kafka; việc đó
thuộc Phase 05.

## Sử dụng local

Đặt credential trong `.env` (không commit file này):

```dotenv
DNSE_API_KEY=...
DNSE_API_SECRET=...
DNSE_API_TOKEN=...
```

Ví dụ gọi market-data endpoint:

```python
from vn30f1m_dataset import DNSEClient

with DNSEClient() as client:
    frame = client.get_ohlcv_futures(
        symbol="VN30F1M",
        from_timestamp=1722502800,
        to_timestamp=1722506400,
        resolution="1m",
    )
```

Kết quả dùng `event_time` timezone-aware UTC, `trading_date` theo
`Asia/Ho_Chi_Minh`, và các cột OHLCV canonical. `source_record_id` được sinh
từ symbol và event time để downstream xây business key.

Client có timeout mặc định 30 giây và retry cho các request GET lỗi tạm thời
(408/429/5xx). Các giá trị này cấu hình qua `DNSE_TIMEOUT_SECONDS`,
`DNSE_MAX_RETRIES` và `DNSE_BACKOFF_FACTOR`.

Endpoint/version được cấu hình qua `DNSE_PUBLIC_URL`, `DNSE_OPENAPI_URL` và
`DNSE_API_VERSION`; không hardcode token trong source. Tham chiếu chính thức:
[DNSE API Platform](https://developers.dnse.com.vn/docs/guide/intro/api_platform/)
và [DNSE Market Data](https://developers.dnse.com.vn/docs/dnse/market-data/).
