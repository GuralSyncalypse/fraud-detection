# Kịch bản demo cuối kỳ (7–10 phút)

## Chuẩn bị trước demo

```powershell
docker compose up -d --wait
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify_cluster.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify_phase5.ps1
```

Mở sẵn:

- NameNode UI: http://localhost:9870
- Spark Master UI: http://localhost:8080
- JupyterLab: http://localhost:8888, token `fraud-demo`
- Notebook `00_full_demo.ipynb`

## Kịch bản trình bày

### 0:00–1:00 — Kiến trúc

1. Chạy `docker compose ps` và chỉ ra 7 container healthy.
2. Mở Spark Master UI, chỉ ra 2 worker ALIVE.
3. Mở NameNode UI, duyệt `/financial/raw`, `/processed`, `/models`, `/predictions`.

Thông điệp: HDFS là nơi lưu dữ liệu chính; Spark đọc/ghi trực tiếp HDFS.

### 1:00–2:30 — Dataset và cleaning

1. Mở phần 1–2 trong `00_full_demo.ipynb`.
2. In schema Parquet và một vài giao dịch đã làm sạch.
3. Hiển thị biểu đồ Fraud vs Normal.

Thông điệp: chỉ khoảng 0,15% giao dịch là Fraud, nên Accuracy có thể gây hiểu lầm.

### 2:30–4:00 — Feature engineering và training

1. Trình bày split Train/Test bằng hash với seed 42.
2. Nhấn mạnh preprocessor chỉ fit trên Train.
3. Chỉ ra `Class` và các ID không có trong 277 features.
4. Nêu ba thí nghiệm imbalance: None, Class Weight và Undersampling.

### 4:00–6:30 — Evaluation

1. Hiển thị bảng model comparison.
2. Hiển thị Precision–Recall curve và confusion matrix.
3. So sánh LR Class Weight với RF None/RF Undersampling.
4. Hiển thị threshold table.

Thông điệp: Class Weight tăng Recall nhưng làm tăng False Positive; không chọn threshold nếu chưa có chi phí nghiệp vụ.

### 6:30–8:00 — Suspicious transactions

1. Đọc `/financial/predictions/suspicious_transactions` bằng Spark.
2. Hiển thị `transaction_id`, `amount`, `fraud_probability`, `status`.
3. Dùng thuật ngữ “giao dịch cần xem xét”, không kết luận gian lận.

### 8:00–9:00 — Dự đoán dữ liệu mới

1. Hiển thị `data/sample/new_transactions.csv` không có cột Class.
2. Đọc `/financial/predictions/new_transactions`.
3. Chỉ ra model được load lại từ HDFS và `model_retrained = false`.

### 9:00–10:00 — Kết luận

- Pipeline chạy end-to-end và artifact tồn tại sau session train.
- Metric lấy từ thực nghiệm, không hard-code trong code đánh giá.
- Hạn chế: compute cap cho training, feature chưa có lịch sử hành vi, threshold chưa gắn với chi phí nghiệp vụ.
- Hướng phát triển: streaming, drift monitoring, explainability và analyst feedback loop.

## Câu hỏi thường gặp

**Tại sao không dùng Accuracy?** Vì Normal chiếm khoảng 99,85%; dự đoán tất cả là Normal vẫn có Accuracy rất cao.

**Tại sao RF None có PR-AUC cao nhưng Recall ở threshold 0,5 rất thấp?** Mô hình có khả năng xếp hạng tương đối tốt, nhưng probability chưa phù hợp với ngưỡng mặc định trong dữ liệu mất cân bằng.

**Isolation Forest có dùng Class để train không?** Không. Class chỉ được dùng sau anomaly scoring để đo mức trùng khớp với fraud thật.

**Feature importance có phải nguyên nhân gây fraud không?** Không. Nó chỉ mô tả mức đóng góp vào quyết định của model trong dữ liệu và cấu hình hiện tại.
