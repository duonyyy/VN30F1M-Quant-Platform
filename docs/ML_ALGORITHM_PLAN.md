# ML Algorithm Plan

Tài liệu này là trọng tâm của `vn30f1m_platform`. Hạ tầng, Spark, ClickHouse và dashboard chỉ có ý nghĩa khi phục vụ được pipeline ML/backtest đáng tin cậy.

Nguồn tham khảo chính:

- `Trading_system/scripts/create_ml_alpha.py`
- `Trading_system/documents/ML_alpha_report.md`
- `Trading_system/src/trading/strategy/ml_xgb.py`
- `Trading_system/configs/model_params.yml`

## 1. Mục tiêu ML

Mục tiêu không phải là tạo dashboard đẹp, mà là xây dựng một pipeline ML có thể trả lời trung thực:

- Model có dự đoán được hướng biến động VN30F1M tốt hơn nhiễu không?
- Tín hiệu Long/Short/Hold sau phí có còn hiệu quả không?
- Kết quả có giữ được trên holdout test không?
- Có bị data leakage, overfit validation hoặc chọn tham số trên test không?

## 2. Bài toán ML

Loại bài toán:

```text
Supervised classification trên dữ liệu intraday VN30F1M
```

Input:

```text
OHLCV intraday + technical/statistical features
```

Output model:

```text
P(up movement đủ lớn trong horizon tương lai)
```

Output trading policy:

```text
LONG, SHORT, HOLD/CLOSE
```

## 3. Timeframe MVP

Timeframe canonical của Phase 01 và MVP:

```text
15m
```

Lý do:

- Theo báo cáo cũ, `logistic_h4` trên `15m` là ML baseline tốt nhất hiện tại.
- `30m` với nhiều feature TA-inspired cho validation đẹp nhưng test yếu, có dấu hiệu overfit/regime shift.

Timeframe sau MVP:

- `5m`: nhiều dữ liệu hơn nhưng nhiễu và phí ảnh hưởng mạnh hơn.
- `30m`: ít nhiễu hơn nhưng có thể bỏ lỡ chuyển động ngắn.
- `raw/1m`: giữ làm cadence nguồn/quality input; chỉ dùng làm timeframe mô hình sau khi pipeline ổn.

## 4. Feature engineering

## 4A. Training framework

**Framework chính:** Apache Spark MLlib qua `pyspark.ml`.

Lý do:

- Đây là đồ án Big Data; cùng một Spark pipeline xử lý dữ liệu và train model.
- Có thể chạy local bằng Spark mode, sau đó mở rộng sang standalone/cluster mà không đổi contract.
- Tránh biến phần train thành một pipeline pandas/scikit-learn tách rời khỏi phần Big Data.

Spark ML pipeline chuẩn sẽ dùng:

```text
Spark DataFrame
  -> Imputer / feature preparation
  -> VectorAssembler
  -> StandardScaler (khi cần)
  -> LogisticRegression / RandomForestClassifier / GBTClassifier
```

`scikit-learn` chỉ được phép dùng cho sanity check hoặc benchmark phụ trên sample nhỏ; không phải pipeline nghiệm thu chính. `ExtraTrees` không phải model native của Spark MLlib nên chỉ để optional comparison nếu giảng viên yêu cầu.

Feature phải chỉ dùng dữ liệu đã biết tại thời điểm ra quyết định.

Nhóm feature chính:

### Return và momentum

```text
ret_1, ret_2, ret_3, ret_5, ret_8, ret_13, ret_21, ret_34, ret_55
mom_points_1, mom_points_2, ..., mom_points_55
intrabar_return
```

### Volatility và range

```text
volatility_5, volatility_10, volatility_20, volatility_40, volatility_80
range_mean_5, range_mean_10, range_mean_20, range_mean_40, range_mean_80
atr_14, atr_20, atr_pct_14, atr_z_80
```

### Technical indicators

```text
rsi_7, rsi_14, rsi_28
adx_8, adx_14
ema_dist_8, ema_dist_13, ema_dist_21, ema_dist_34, ema_dist_55
bb_width, bb_pos, bb_break_up, bb_break_down
```

### Linear regression + ATR

```text
lr_atr_z_10, lr_atr_z_14, lr_atr_z_20, lr_atr_z_30, lr_atr_z_40
lr_atr_z_abs_*
lr_atr_z_mom1_*
lr_atr_z_mom3_*
lr_atr_cross_up_*
lr_atr_cross_down_*
```

### Volume và session

```text
volume_z_10, volume_z_20, volume_z_50, volume_z_100
relative_volume_10, relative_volume_20, relative_volume_50, relative_volume_100
hour, minute, bar_of_day, morning_session, afternoon_session
```

## 5. Chống leakage

Quy tắc bắt buộc:

```python
raw_feature_cols = [...]
features[raw_feature_cols] = features[raw_feature_cols].shift(1)
```

Ý nghĩa:

```text
Tại thời điểm t, model chỉ được nhìn dữ liệu đã biết đến t-1.
```

Cấm:

- Tính feature bằng `Close[t+horizon]`.
- Fit scaler/imputer/feature selector trên toàn bộ data.
- Chọn threshold trên test set.
- Random split dữ liệu chuỗi thời gian.
- Báo cáo validation như kết quả cuối.

## 6. Label và target

Target dùng forward return:

```python
future_return = Close.shift(-horizon) / Close - 1
```

Label:

```python
if future_return > threshold:
    label = 1
elif future_return < -threshold:
    label = 0
else:
    label = NaN
```

Các dòng `label = NaN` bị loại khỏi training vì biến động quá nhỏ, dễ bị phí và nhiễu nuốt mất.

MVP candidate chính:

```text
timeframe = 15m
horizon = 4
label_threshold = 0.0012
```

Diễn giải:

```text
Dự báo biến động khoảng 60 phút tiếp theo.
Chỉ train trên movement lớn hơn khoảng +/-0.12%.
```

## 7. Split dữ liệu

Split chuẩn:

| Split | Thời gian | Vai trò |
|---|---|---|
| Train | trước `2022-01-01` | Fit model, scaler, imputer, feature selection |
| Validation | `2022-01-01` đến trước `2024-01-01` | Chọn model, threshold, policy |
| Test/Holdout | từ `2024-01-01` trở đi | Đánh giá cuối cùng |

Nếu dữ liệu không đủ, fallback split theo tỷ lệ thời gian:

```text
60% train -> 20% validation -> 20% test
```

Không dùng random split.

## 8. Feature selection

MVP dùng Spark MLlib:

```text
Imputer(strategy="median")
ChiSqSelector(numTopFeatures=N) trên train set
chọn top N feature
```

`ChiSqSelector` là lựa chọn native của Spark MLlib cho feature selection. Nếu cần mutual information đúng theo báo cáo legacy, chỉ chạy như benchmark phụ trên sample train; không đưa scikit-learn vào pipeline chính.

Top N gợi ý:

| Model | Top features |
|---|---:|
| Logistic Regression | 36-42 |
| Random Forest | 44 |
| GBT/One-vs-Rest | 24 |

Lưu ý: feature selection phải fit trên train set בלבד. Validation/test chỉ transform theo feature đã chọn.

## 9. Model candidates

MVP ưu tiên model đơn giản trước.

### Candidate 1: Spark MLlib Logistic Regression

Model chính ban đầu:

```python
pyspark.ml.classification.LogisticRegression(
    featuresCol="features",
    labelCol="label",
    regParam=0.08,
    elasticNetParam=0.0,
    maxIter=200,
    standardization=True,
)
```

Nếu cần cân bằng lớp, tạo `weightCol` trong Spark DataFrame và truyền vào estimator; Spark không dùng tham số `class_weight` như scikit-learn.

Lý do:

- Ít overfit hơn tree models.
- Dễ giải thích.
- Theo kết quả cũ, `logistic_h4` có holdout tốt nhất.

### Candidate 2: Spark MLlib Random Forest

```python
pyspark.ml.classification.RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=200,
    maxDepth=5,
    minInstancesPerNode=80,
    featureSubsetStrategy="sqrt",
    seed=42,
)
```

Mục đích: kiểm tra quan hệ phi tuyến.

### Candidate 3: Spark MLlib Gradient-Boosted Trees

Dùng sau Logistic Regression và Random Forest để kiểm tra mô hình phi tuyến. `GBTClassifier` native chủ yếu dành cho binary classification; với `LONG/SHORT/HOLD` phải dùng `OneVsRest` hoặc giữ GBT ở mức optional.

### Candidate 4: Optional ExtraTrees/XGBoost

Dùng sau khi baseline Spark MLlib ổn và chỉ khi cần so sánh thêm. Cần kiểm soát overfit chặt vì kết quả cũ cho thấy validation đẹp nhưng test yếu.

### Candidate để sau MVP

- XGBoost (`xgboost.spark` nếu cần giữ Spark DataFrame).
- LightGBM.
- LSTM.
- Ensemble.

Không nên bắt đầu bằng LSTM vì dữ liệu tài chính intraday nhiễu, dễ overfit, khó giải thích.

## 10. Signal policy

Model chỉ tạo xác suất:

```text
ml_prob = P(label = 1)
```

Position được tạo bởi policy:

```text
LONG nếu ml_prob >= long_threshold
SHORT nếu ml_prob <= short_threshold
HOLD/CLOSE nếu nằm giữa hoặc chạm exit rule
```

Policy parameters:

```text
long_threshold
short_threshold
min_hold_bars
exit_buffer
```

Các tham số này chỉ được chọn trên validation set.

Objective chọn policy:

```text
validation_sharpe
+ 0.002 * validation_profit_per_year
- turnover_penalty
```

Mục tiêu: tránh model trade quá nhiều để lấy Sharpe ảo.

## 11. Backtest metrics

Metrics bắt buộc:

- Sharpe after fee.
- Profit after fee.
- Profit per year.
- Margin.
- Maximum drawdown.
- Hit rate.
- Trade count.
- Trades per day.

Fee phải được tính trong backtest. Báo cáo không sau phí là không đủ tin cậy.

## 12. Kết quả baseline hiện tại từ `Trading_system`

Baseline tốt nhất đã ghi nhận:

```text
Model: logistic_h4
Timeframe: 15m
Horizon: 4
Label threshold: 0.0012
```

Holdout test:

| Metric | Kết quả |
|---|---:|
| Sharpe after fee | 1.508 |
| Profit/year | 86.4 |
| Hitrate | 62.3% |
| Margin | 9.6 |

Đánh giá thẳng:

- Có tín hiệu dương trên test.
- Chưa đạt chuẩn alpha mạnh.
- Chưa nên tuyên bố production trading.
- Rất phù hợp làm ML baseline đầu tiên cho project mới.

## 13. Tiêu chí nghiệm thu ML MVP

MVP pass khi:

- Pipeline train/validation/test chạy lại được trong `vn30f1m_platform`.
- Feature được shift để chống leakage.
- Scaler/imputer/feature selector chỉ fit trên train.
- Threshold/policy chỉ chọn trên validation.
- Test/holdout chỉ dùng để báo cáo cuối.
- Có report markdown cho từng candidate.
- Có ít nhất một model baseline chạy ra signal và backtest metrics.

MVP fail khi:

- Dùng random split.
- Dùng test set để chọn threshold.
- Dashboard báo metric validation như metric test.
- Feature chưa shift.
- XGBoost cũ dùng past return làm label nhưng gọi là future target.

## 14. Phase ML ưu tiên

```text
ML-01: Migrate feature builder từ create_ml_alpha.py
ML-02: Migrate label builder forward return
ML-03: Migrate time split
ML-04: Migrate Spark `ChiSqSelector` feature selection
ML-05: Migrate Logistic Regression baseline
ML-06: Migrate signal policy
ML-07: Migrate backtest metrics
ML-08: Generate ML report
ML-09: Add Spark Random Forest / GBT comparison; ExtraTrees optional
ML-10: Optional XGBoost re-benchmark sau khi sửa label
```

## 15. Cảnh báo quan trọng

Một sự thật cần nói rõ: phần ML hiện tại chưa chứng minh được alpha đủ mạnh để trading thật. Giá trị lớn nhất hiện tại là pipeline nghiên cứu đã có hướng đúng: split theo thời gian, chống leakage, validation policy và holdout test.

Vì vậy ưu tiên của project mới là làm pipeline ML sạch, tái lập được, rồi mới cải thiện model.
