# Báo cáo đồ án: Phân tích và phát hiện giao dịch tài chính bất thường

Tài liệu này là nội dung nền để chuyển sang Word/PDF. Các số liệu thực nghiệm được lấy từ `output/results/*.json` và `*.csv` sau khi chạy pipeline, không phải kết quả giả lập.

## CHƯƠNG 1 – TỔNG QUAN

### 1.1 Đặt vấn đề

Khối lượng giao dịch tài chính lớn và tỷ lệ gian lận rất thấp tạo ra hai thách thức: xử lý dữ liệu vượt khả năng của công cụ đơn máy thông thường và đánh giá mô hình trong điều kiện mất cân bằng mạnh. Đồ án sử dụng HDFS làm lớp lưu trữ phân tán, Spark làm lớp xử lý và Machine Learning để tạo điểm rủi ro/gắn cờ giao dịch cần xem xét.

### 1.2 Mục tiêu

- Xây dựng pipeline CSV → HDFS → Spark → Parquet → ML → prediction.
- So sánh Logistic Regression, Random Forest và Isolation Forest.
- Đánh giá tác động của Class Weight và Random Undersampling.
- Lưu/load model, prediction và thực hiện inference cho dữ liệu mới.
- Chạy toàn bộ hệ thống trên Docker Compose để dễ tái lập và demo.

### 1.3 Phạm vi

Đồ án dùng dữ liệu giao dịch ẩn danh. Model tạo tín hiệu hỗ trợ rà soát, không thay thế kết luận nghiệp vụ. Hệ thống là batch demo trên máy cá nhân, chưa phải production real-time.

### 1.4 Công nghệ

Hadoop 3.4.3, Spark/PySpark 3.5.9, Java 17, Python, Spark MLlib, scikit-learn, pandas, matplotlib, JupyterLab và Docker Compose.

## CHƯƠNG 2 – CƠ SỞ LÝ THUYẾT

### 2.1–2.4 Big Data, Hadoop, HDFS và Spark

HDFS chia file thành block và nhân bản trên DataNode; NameNode quản lý metadata. Trong project, replication factor bằng 2 trên hai DataNode. Spark Master phân phối task tới hai worker; Spark đọc/ghi trực tiếp `hdfs:///financial/...`.

### 2.5 Machine Learning và 2.6 Imbalanced Data

Fraud chỉ chiếm khoảng 0,1495% dữ liệu có nhãn. Accuracy vì vậy không phản ánh khả năng phát hiện lớp hiếm. Đồ án ưu tiên Precision, Recall, F1, PR-AUC và confusion matrix của Fraud.

### 2.7 Logistic Regression

Mô hình tuyến tính xác suất được dùng làm baseline. Class Weight điều chỉnh đóng góp của mỗi lớp trong hàm mất mát.

### 2.8 Random Forest

Random Forest kết hợp nhiều decision tree. Đồ án dùng 25 cây, max depth 8 và thử None, Class Weight, Undersampling. Feature importance không được diễn giải như quan hệ nhân quả.

### 2.9 Isolation Forest

Isolation Forest là phương pháp unsupervised, cô lập điểm bất thường bằng các partition ngẫu nhiên. Model không nhận `Class` trong feature; nhãn chỉ dùng để đánh giá sau scoring.

### 2.10 Evaluation Metrics

- Precision = TP / (TP + FP).
- Recall = TP / (TP + FN).
- F1 = trung bình điều hòa của Precision và Recall.
- PR-AUC đo chất lượng xếp hạng trong bài toán lớp dương hiếm.

## CHƯƠNG 3 – PHÂN TÍCH VÀ THIẾT KẾ

### 3.1 Dataset

Lần chạy hiện tại có 13.305.915 giao dịch sạch, 8.914.963 giao dịch có nhãn và 13.332 Fraud. Dữ liệu chứa thời gian, amount, transaction method, merchant location, MCC và errors.

### 3.2–3.4 System, HDFS và Spark Architecture

Docker Compose cung cấp NameNode, hai DataNode, Spark Master, hai Spark Worker và Jupyter. Các service giao tiếp trong `financial-fraud-network`; HDFS dùng persistent volumes.

### 3.5 Data Pipeline

```text
CSV + JSON labels → HDFS raw → Spark cleaning → Parquet processed
→ deterministic Train/Test → feature pipeline → model → HDFS prediction
```

### 3.6 ML Pipeline

Các feature số gồm amount, hour, day-of-week, month, year, weekend, online và error. Các feature phân loại gồm use_chip, MCC và merchant state, được StringIndexer + OneHotEncoder. StandardScaler và encoder chỉ fit trên Train.

## CHƯƠNG 4 – TRIỂN KHAI

### 4.1–4.4 Docker, Hadoop, Spark và HDFS

Chi tiết lệnh triển khai nằm trong `README.md`. HDFS có `/financial/raw`, `/processed`, `/models` và `/predictions`. Healthcheck bảo đảm dependency chỉ khởi động sau khi service nền sẵn sàng.

### 4.5 Data Cleaning

Spark áp dụng schema tường minh, chuẩn hóa timestamp/amount, kiểm tra null và loại duplicate theo transaction ID. Dữ liệu sạch được lưu Parquet partition theo năm.

### 4.6 Feature Engineering

Feature vector có 277 chiều trong lần fit hiện tại. `Class` và các identifier không nằm trong vector. Seed 42 được dùng để tái lập split/sampling.

### 4.7 Imbalance Handling

- None: giữ phân bố tự nhiên.
- Class Weight: Fraud nhận trọng số lớn hơn.
- Undersampling: giữ toàn bộ Fraud Train và giảm Normal về gần tỷ lệ 10:1.
- Test Set không bị sampling.

### 4.8 Model Training

Model Spark và preprocessor được lưu vào `/financial/models`. Isolation Forest được lưu dạng joblib rồi copy lên HDFS. Verifier load lại artifact trong Spark application mới.

## CHƯƠNG 5 – THỰC NGHIỆM

### 5.1 Dataset Statistics

- Train: 7.132.229 dòng, 10.642 Fraud.
- Test: 1.782.734 dòng, 2.690 Fraud.
- Test không sampling và được dùng giống nhau cho cả sáu model.

### 5.2–5.4 Kết quả mô hình

| Model | Imbalance | Precision | Recall | F1 | PR-AUC |
|---|---|---:|---:|---:|---:|
| Logistic Regression | None | 0,7050 | 0,2141 | 0,3285 | 0,2277 |
| Logistic Regression | Class Weight | 0,0323 | 0,9249 | 0,0624 | 0,2250 |
| Random Forest | None | 1,0000 | 0,0015 | 0,0030 | 0,5319 |
| Random Forest | Class Weight | 0,0141 | 0,8892 | 0,0278 | 0,0893 |
| Random Forest | Undersampling | 0,3464 | 0,2394 | 0,2831 | 0,2492 |
| Isolation Forest | Unsupervised | 0,0030 | 0,7743 | 0,0060 | 0,0085 |

### 5.5 Imbalance Experiments

Class Weight tăng Recall của LR từ 21,41% lên 92,49%, nhưng Precision giảm từ 70,50% xuống 3,23%. RF Class Weight có hiện tượng tương tự. RF Undersampling tại threshold 0,5 tạo cân bằng hơn giữa Precision 34,64% và Recall 23,94%.

### 5.6–5.8 Confusion Matrix, Precision/Recall/F1 và PR-AUC

RF None có PR-AUC 0,5319 nhưng tại threshold 0,5 chỉ phát hiện 4/2.690 Fraud. Điều này cho thấy khả năng xếp hạng và quyết định tại một threshold là hai vấn đề khác nhau.

### 5.9 Threshold Analysis

Với RF None, threshold 0,2 cho Precision 85,49%, Recall 28,92% và F1 43,22%; threshold 0,5 cho Precision 100% nhưng Recall chỉ 0,15%. Không kết luận threshold tối ưu khi chưa biết chi phí nghiệp vụ.

### 5.10 Suspicious Transactions

Prediction được lưu ở `/financial/predictions`. Top 20 giao dịch theo RF Undersampling được lưu ở `/financial/predictions/suspicious_transactions`. Trạng thái sử dụng là `NEEDS_REVIEW`, không phải “confirmed fraud”.

## CHƯƠNG 6 – KẾT LUẬN

### 6.1 Kết quả đạt được

Pipeline end-to-end đã chạy và được xác minh: dữ liệu lên HDFS, Spark cleaning/feature engineering, train nhiều model, lưu/load model, đánh giá trên Test thật, lưu prediction, tạo biểu đồ và dự đoán dữ liệu mới không có nhãn.

### 6.2 Hạn chế

- Supervised training dùng compute cap khoảng 500.000 dòng để phù hợp máy cá nhân.
- Feature chưa khai thác lịch sử hành vi theo tài khoản/thẻ.
- Probability chưa calibration và threshold chưa gắn với chi phí nghiệp vụ.
- Batch pipeline chưa xử lý streaming và model drift.

### 6.3 Hướng phát triển

- Spark Structured Streaming hoặc Kafka cho near-real-time scoring.
- Feature theo cửa sổ thời gian và hành vi lịch sử.
- Probability calibration, cost-sensitive threshold và explainability.
- Monitoring drift, model registry và analyst feedback loop.
