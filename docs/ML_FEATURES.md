# Phase 06 – ML feature engineering

Phase 06 tạo Gold features từ Silver canonical OHLCV. Hiện feature builder dùng
pandas/Arrow để kiểm chứng công thức và contract trên dataset local; Spark 05B
vẫn là lớp Bronze/Silver, còn Spark-distributed feature execution sẽ tối ưu ở
giai đoạn dữ liệu lớn hơn. Feature row tại bar `t`
được shift một bar, vì vậy model chỉ nhận dữ liệu đã biết trước thời điểm ra
quyết định tại `t`.

## Chạy

```powershell
python -m vn30f1m_core.cli analysis build-features --json
```

Mặc định:

- Input: `lakehouse/silver/vn30f1m/ohlcv_intraday`.
- Output: `lakehouse/gold/vn30f1m/features`.
- Version: `vn30f1m_features_v1`.

Có thể chỉ định input/output:

```powershell
python -m vn30f1m_core.cli analysis build-features `
  --input "lakehouse\silver\vn30f1m\ohlcv_intraday" `
  --output "lakehouse\gold\vn30f1m\features" `
  --feature-set-version vn30f1m_features_v1 `
  --shift-bars 1 `
  --json
```

## Nhóm feature

- Return, momentum, close z-score.
- Volatility, range, ATR và ATR regime.
- RSI, ADX, EMA distance.
- Bollinger width/position/breakout.
- Linear-regression midline chuẩn hóa theo ATR.
- Candle body/wick/intrabar return.
- Volume z-score và relative volume.
- Session/hour/minute features theo `Asia/Ho_Chi_Minh`.

Các row đầu chưa đủ rolling window được giữ lại với `feature_status=warmup`;
Phase 06B sẽ loại chúng khi tạo dataset train. Feature không chứa `label` hoặc
forward return để tránh trộn target vào Gold feature input.
