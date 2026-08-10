# Data Contracts

Contract này là nguồn chuẩn cho tên cột, kiểu dữ liệu, khóa và quy tắc trao đổi giữa các module. Code mới phải dùng contract canonical; adapter mới được phép xử lý tên cột legacy như `Date`, `Open`, `High`, `Low`, `Close`, `Volume`.

## 1. Phạm vi và quy ước chung

### 1.1 Timeframe

| Giá trị | Ý nghĩa | Vai trò |
|---|---|---|
| `1m` | Nến nguồn/chi tiết | Lưu và kiểm tra dữ liệu gốc khi nguồn cung cấp |
| `5m` | Nến derived | Nghiên cứu sau MVP |
| `15m` | Nến derived | **Canonical cho phân tích, ML và backtest MVP** |
| `30m` | Nến derived | Nghiên cứu sau MVP |

Mỗi bảng phải có `timeframe`. Không được trộn các timeframe trong cùng một partition hoặc một run phân tích.

### 1.2 Khóa và thời gian

Business key của dữ liệu theo bar:

```text
symbol + event_time + timeframe
```

Quy ước:

- `symbol` là lowercase trong tên cột, giá trị chuẩn mặc định là `VN30F1M`.
- `event_time` là **thời điểm bắt đầu bar**, kiểu timestamp có timezone và được lưu theo UTC.
- `trading_date` là ngày giao dịch theo timezone `Asia/Ho_Chi_Minh`, được derive từ `event_time`; dùng để partition và kiểm tra phiên.
- `ingested_at`, `created_at` và `available_at` đều là timestamp UTC.
- Không dùng timestamp naive trong output canonical.

### 1.3 Kiểu dữ liệu và tên cột

- Dùng `snake_case`, viết thường; không dùng `Open`, `Close`, `Datetime` trong bảng canonical.
- Giá và volume dùng `float64` trong Parquet/Spark để tương thích với pipeline hiện tại.
- Cờ dùng `bool`; số đếm dùng `int64`; xác suất/confidence nằm trong `[0, 1]`.
- Giá trị thiếu dùng `null`, không dùng `0`, chuỗi rỗng hoặc `NaN` để thay thế ngầm.
- Schema version được quản lý ở metadata của dataset/run; khi đổi meaning hoặc đổi kiểu phải tăng major version.

### 1.4 Storage và idempotency

- Format chuẩn: Parquet; MinIO chỉ thay đổi storage backend, không thay đổi schema.
- Partition đề xuất: `symbol/timeframe/trading_date`.
- Landing là immutable; silver/gold có thể rebuild từ input versioned.
- Một run phải có `ingest_run_id` hoặc `pipeline_run_id`; chạy lại cùng input/config không tạo bản ghi trùng business key.
- Mọi bảng output phải có thể truy nguyên về source và run tạo ra nó.

## 2. Contract: `ohlcv_intraday`

Owner: `vn30f1m_dataset`  
Producer downstream: `pipelines/batch_jobs`  
Canonical MVP: `timeframe = 15m`

### Schema

```text
symbol             String       NOT NULL
event_time         TimestampUTC NOT NULL  # bar start
trading_date       Date         NOT NULL  # Asia/Ho_Chi_Minh
timeframe          String       NOT NULL  # 1m|5m|15m|30m
open               Float64      NOT NULL
high               Float64      NOT NULL
low                Float64      NOT NULL
close              Float64      NOT NULL
volume             Float64      NOT NULL
source             String       NOT NULL
source_record_id   String       NULL
ingest_run_id      String       NOT NULL
ingested_at        TimestampUTC NOT NULL
quality_status     String       NOT NULL  # valid|suspect|rejected
```

### Validation

- `symbol`, `event_time`, `timeframe` tạo thành unique key.
- `open`, `high`, `low`, `close` phải hữu hạn và lớn hơn `0`.
- `high >= max(open, close, low)` và `low <= min(open, close, high)`.
- `volume >= 0`; volume bằng `0` phải được giữ nguyên và ghi nhận trong quality report, không tự đổi thành null.
- Dữ liệu phải sort tăng dần theo `event_time` trong mỗi `symbol/timeframe`.
- Không forward-fill OHLCV qua khoảng nghỉ phiên.
- `quality_status = rejected` không được đi vào silver/gold.

### Mapping legacy tối thiểu

```text
Date/Datetime -> event_time
Open          -> open
High          -> high
Low           -> low
Close         -> close
Volume        -> volume
```

## 2B. Contract: Kafka topic `vn30f1m.ohlcv.raw`

Owner: `vn30f1m_dataset`  
Consumer: Kafka landing/bronze consumer  
MVP status: bắt buộc

Message key:

```text
symbol + event_time + timeframe
```

Message value:

```json
{
  "schema_version": "ohlcv_raw_v1",
  "symbol": "VN30F1M",
  "event_time": "2026-08-06T02:15:00Z",
  "trading_date": "2026-08-06",
  "timeframe": "1m",
  "open": 1234.5,
  "high": 1236.0,
  "low": 1234.0,
  "close": 1235.2,
  "volume": 1000.0,
  "source": "historical_csv",
  "source_record_id": "optional",
  "ingest_run_id": "20260806_120000",
  "published_at": "2026-08-06T05:00:00Z"
}
```

Validation:

- Message key phải khớp với `symbol`, `event_time`, `timeframe` trong value.
- `schema_version` bắt buộc để consumer biết cách parse.
- `event_time` và `published_at` là UTC.
- Record lỗi schema phải đi vào dead-letter topic `vn30f1m.ohlcv.raw.dlq`.
- Consumer phải idempotent theo message key, không ghi trùng khi replay.

## 3. Contract: `vn30f1m_features`

Owner: `vn30f1m_analysis`  
Input: `ohlcv_intraday` từ silver/canonical timeframe

### Schema

```text
symbol                String       NOT NULL
event_time            TimestampUTC NOT NULL
trading_date          Date         NOT NULL
timeframe             String       NOT NULL
open                  Float64      NOT NULL
high                  Float64      NOT NULL
low                   Float64      NOT NULL
close                 Float64      NOT NULL
volume                Float64      NOT NULL
feature_set_version   String       NOT NULL
available_at          TimestampUTC NOT NULL
atr_14                Float64      NULL
rsi_14                Float64      NULL
bb_upper              Float64      NULL
bb_middle             Float64      NULL
bb_lower              Float64      NULL
bb_width              Float64      NULL
lr_midline_14         Float64      NULL
z_score               Float64      NULL
ema_20                Float64      NULL
ema_slope             Float64      NULL
adx_14                Float64      NULL
close_lag_1           Float64      NULL
close_lag_2           Float64      NULL
close_lag_3           Float64      NULL
close_lag_4           Float64      NULL
close_lag_5           Float64      NULL
return_1              Float64      NULL
return_2              Float64      NULL
return_3              Float64      NULL
volume_change         Float64      NULL
hour                  Int64        NULL
day_of_week           Int64        NULL
is_session_end        Bool         NULL
```

### Validation và leakage rule

- Khóa vẫn là `symbol + event_time + timeframe`.
- Null ở các row đầu do rolling window là hợp lệ; row chưa đủ feature không được dùng để train/backtest.
- Feature row tại bar `t` chỉ dùng dữ liệu OHLCV đến hết bar `t`; `available_at` không sớm hơn thời điểm đóng bar.
- Quyết định tại bar kế tiếp mới được phép dùng feature của bar `t`; tuyệt đối không dùng feature chứa dữ liệu tương lai.
- `future_return`, `label`, test outcome và signal không được xuất hiện trong feature input.
- Mọi thay đổi danh sách/công thức feature phải tăng `feature_set_version`.

## 4. Contract: `vn30f1m_labels`

Owner: `vn30f1m_analysis`  
Usage: train/evaluate only; không phải input của inference realtime.

```text
symbol             String       NOT NULL
event_time         TimestampUTC NOT NULL
trading_date       Date         NOT NULL
timeframe          String       NOT NULL
horizon_bars       Int64        NOT NULL
threshold          Float64      NOT NULL
future_return      Float64      NULL
label              String       NULL  # LONG|SHORT|HOLD
label_version      String       NOT NULL
created_at          TimestampUTC NOT NULL
```

Validation:

- `future_return = close[t + horizon_bars] / close[t] - 1` trên cùng symbol/timeframe.
- Không đủ horizon ở cuối dataset thì `future_return` và `label` là null; các row này bị loại khỏi train/evaluate.
- `LONG` nếu forward return vượt threshold; `SHORT` nếu thấp hơn âm threshold; còn lại `HOLD`.
- Threshold/horizon phải nằm trong metadata của run và không được thay đổi ngầm giữa các tập.
- Label không được tính trước khi split theo thời gian theo cách làm lộ thông tin validation/test vào train.

## 5. Contract: `vn30f1m_signals`

Owner: `vn30f1m_analysis`  
Usage: strategy output và backtest input.

```text
pipeline_run_id     String       NOT NULL
strategy_name       String       NOT NULL
strategy_version    String       NOT NULL
symbol              String       NOT NULL
event_time          TimestampUTC NOT NULL
timeframe           String       NOT NULL
signal              String       NOT NULL  # LONG|SHORT|HOLD|CLOSE
position            Int64        NOT NULL  # -1|0|1
confidence          Float64      NULL
close               Float64      NOT NULL
reason              String       NULL
model_version       String       NULL
created_at          TimestampUTC NOT NULL
```

Validation:

- Unique key tối thiểu: `pipeline_run_id + strategy_name + symbol + event_time + timeframe`.
- `position` chỉ nhận `-1`, `0`, `1`; signal và position phải nhất quán theo policy của strategy.
- `confidence` nếu có phải nằm trong `[0, 1]`.
- Signal tại bar `t` chỉ dùng feature có `available_at <= thời điểm ra quyết định`.
- Contract này không đại diện cho lệnh đã khớp; fill, phí và slippage thuộc backtest/trade execution report ở phase sau.

## 6. Contract: `backtest_metrics`

Owner: `vn30f1m_analysis`

```text
pipeline_run_id     String       NOT NULL
strategy_name       String       NOT NULL
strategy_version    String       NOT NULL
timeframe           String       NOT NULL
period_name         String       NOT NULL  # train|validation|test|full
start_event_time    TimestampUTC NOT NULL
end_event_time      TimestampUTC NOT NULL
fee_model_version   String       NOT NULL
sharpe_after_fee    Float64      NULL
margin              Float64      NULL
max_drawdown        Float64      NULL
hit_rate            Float64      NULL
trade_count         Int64        NOT NULL
profit_after_fee    Float64      NULL
created_at           TimestampUTC NOT NULL
```

Validation:

- `start_event_time < end_event_time`; period, timeframe và fee model phải được ghi rõ.
- Metric có thể null khi không đủ lệnh, nhưng report phải nêu nguyên nhân; không thay null bằng `0`.
- Chỉ so sánh các run có cùng timeframe, period definition và fee model.
- Test/holdout report phải được tạo riêng và không được dùng để chọn strategy/model.

## 7. Contract checklist cho mọi pipeline mới

- [ ] Có owner, producer, consumer và version.
- [ ] Có business key và quy tắc deduplication.
- [ ] Có timezone/event-time semantics.
- [ ] Có schema, nullability và allowed values.
- [ ] Có quality checks và failure behavior.
- [ ] Có provenance (`source`, run id, created/ingested time).
- [ ] Có test chứng minh không leakage nếu contract chứa feature/label/signal.
