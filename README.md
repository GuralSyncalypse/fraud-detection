# Phát hiện giao dịch tài chính bất thường

Đồ án Big Data + Machine Learning dùng Hadoop HDFS để lưu dữ liệu, Apache Spark/PySpark để xử lý phân tán và các mô hình ML để gắn cờ giao dịch cần kiểm tra. Toàn bộ hệ thống chạy trên máy cá nhân bằng Docker Compose; không cần cài Java, Hadoop hay Spark trực tiếp trên máy.

Trạng thái hiện tại:

- ✅ Phase 1: HDFS, Spark cluster và Jupyter.
- ✅ Phase 2: upload, EDA, cleaning và CSV → Parquet.
- ✅ Phase 3: feature engineering, train/test, imbalance handling và training.
- ✅ Phase 4: Precision/Recall/F1, confusion matrix, PR-AUC và threshold analysis.
- ✅ Phase 5: biểu đồ, suspicious transactions, dự đoán dữ liệu mới và full demo.

> Model chỉ tạo tín hiệu `NEEDS_REVIEW`. Không được xem prediction là kết luận một giao dịch chắc chắn gian lận.

## 1. Kiến trúc

```text
Dataset local
     │ upload một lần
     ▼
HDFS NameNode ── DataNode 1 + DataNode 2
     │
     ▼
Spark Master ── Worker 1 + Worker 2
     │
     ├── Cleaning / Parquet / Feature Engineering
     ├── Logistic Regression / Random Forest
     ├── distributed inference cho Isolation Forest
     └── Evaluation / Prediction
     │
     ▼
JupyterLab + output JSON/CSV + HDFS predictions/models
```

Các container dùng chung network `financial-fraud-network`. HDFS có persistent named volumes và replication factor `2`; dữ liệu không mất khi restart container hoặc chạy `docker compose down` không kèm `-v`.

## 2. Phiên bản công nghệ

| Thành phần | Phiên bản |
|---|---:|
| Hadoop | 3.4.3 |
| Spark / PySpark | 3.5.9 |
| Scala | 2.12 |
| Java | 17 |
| JupyterLab | 4.3.6 |
| pandas | 2.2.3 |
| scikit-learn | 1.6.1 |
| matplotlib | 3.10.0 |

Các version đã được cố định trong Dockerfile và `requirements.txt` để kết quả có thể tái lập.

## 3. Yêu cầu máy

- Docker Desktop đang chạy ở chế độ Linux containers.
- Docker Compose 2.20 trở lên.
- Tối thiểu 4 CPU, 8 GB RAM trống và khoảng 10 GB disk.
- Windows dùng PowerShell; Linux/macOS/Git Bash dùng Bash.
- Dataset đặt trong `data/raw/`; dataset lớn không được commit vào Git.

## 4. Dataset cần chuẩn bị

Pipeline hiện dùng bộ giao dịch ẩn danh có cấu trúc:

```text
transactions_data.csv
  id,date,client_id,card_id,amount,use_chip,merchant_id,
  merchant_city,merchant_state,zip,mcc,errors

train_fraud_labels.json
  map transaction ID -> "Yes" hoặc "No"

mcc_codes.json
  map MCC -> mô tả ngành hàng (tùy chọn)
```

Đặt file vào đúng vị trí:

```text
data/raw/transactions_data.csv
data/raw/train_fraud_labels.json
data/raw/mcc_codes.json
```

Project chủ động không upload `cards_data.csv`, `cards_data-selected-columns.csv` hoặc `users_data.csv`, vì các file đó có thể chứa số thẻ, CVV, địa chỉ hoặc thông tin cá nhân không cần cho mô hình.

File nhãn JSON lớn sẽ được script chuyển theo kiểu streaming thành `data/staging/fraud_labels.csv`. Không sampling hoặc loại bỏ nhãn trong bước chuyển đổi.

## 5. Cấu trúc repository

```text
fraud-detection/
├── compose.yaml
├── docker/
│   ├── hadoop/
│   └── spark/
├── data/
│   ├── raw/                 # dataset nguồn, không commit
│   ├── staging/             # label CSV tạm
│   └── sample/
├── notebooks/
│   ├── 00_full_demo.ipynb
│   ├── 00_infrastructure_check.ipynb
│   ├── 01_data_exploration.ipynb
│   ├── 02_data_cleaning.ipynb
│   ├── 03_feature_engineering.ipynb
│   ├── 04_logistic_regression.ipynb
│   ├── 05_random_forest.ipynb
│   ├── 06_isolation_forest.ipynb
│   ├── 07_model_evaluation.ipynb
│   └── 08_visualization.ipynb
├── src/
│   ├── preprocessing.py
│   ├── feature_engineering.py
│   ├── train_models.py
│   ├── anomaly_detection.py
│   ├── evaluation.py
│   ├── evaluate_models.py
│   ├── predict.py
│   ├── visualization.py
│   └── verify_*.py
├── scripts/
│   ├── start.* / stop.*
│   ├── init_hdfs.* / upload_hdfs.*
│   ├── run_phase2.* ... run_phase5.*
│   ├── predict_new_transactions.*
│   └── verify_cluster.* ... verify_phase5.*
├── docs/
│   ├── DEMO_SCRIPT.md
│   └── REPORT.md
└── output/
    ├── models/
    ├── results/
    └── figures/
```

## 6. Chạy nhanh từ đầu

Thứ tự bắt buộc là Phase 1 → 2 → 3 → 4 → 5. Không chạy Phase 3 khi chưa có Parquet của Phase 2, không chạy Phase 4 khi chưa có model của Phase 3 và không chạy Phase 5 khi chưa có prediction của Phase 4.

```bash
git clone <repository-url>
cd fraud-detection
```

Sau khi clone, tải dataset theo giấy phép của nguồn dữ liệu và đặt ba file cần thiết vào `data/raw/` như mục 4. Repository không phân phối dataset dung lượng lớn.

### Windows PowerShell

```powershell
# 1. Khởi động cluster, tạo thư mục HDFS và smoke test
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\start.ps1

# 2. Sau khi đặt dataset vào data/raw, upload lên HDFS
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\upload_hdfs.ps1

# 3. Cleaning và tạo Parquet
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_phase2.ps1

# 4. Feature engineering và train model
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_phase3.ps1

# 5. Đánh giá và lưu prediction
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_phase4.ps1

# 6. Xác minh kết quả đánh giá
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify_phase4.ps1

# 7. Visualization, full demo và dự đoán dữ liệu mới
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_phase5.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify_phase5.ps1
```

### Linux, macOS hoặc Git Bash

```bash
chmod +x scripts/*.sh
./scripts/start.sh
./scripts/upload_hdfs.sh
./scripts/run_phase2.sh
./scripts/run_phase3.sh
./scripts/run_phase4.sh
./scripts/verify_phase4.sh
./scripts/run_phase5.sh
./scripts/verify_phase5.sh
```

Các script đều có thể chạy lại. Output Spark dùng chế độ overwrite; upload HDFS bỏ qua file đã tồn tại đúng kích thước.

## 7. Phase 1 – Infrastructure

Khởi động riêng bằng Docker Compose nếu không dùng script tổng hợp:

```bash
docker compose up -d --build --wait
./scripts/init_hdfs.sh
./scripts/verify_cluster.sh
```

PowerShell dùng các file `.ps1` tương ứng.

Web UI:

| Thành phần | URL | Kết quả cần thấy |
|---|---|---|
| HDFS NameNode | http://localhost:9870 | 2 live DataNode, thư mục `/financial` |
| Spark Master | http://localhost:8080 | 2 worker ALIVE |
| Spark Worker 1 | http://localhost:8081 | worker detail |
| Spark Worker 2 | http://localhost:8082 | worker detail |
| JupyterLab | http://localhost:8888 | token mặc định `fraud-demo` |

Để đổi Jupyter token:

```bash
cp .env.example .env
# sửa JUPYTER_TOKEN trong .env rồi restart Jupyter
```

Smoke test chạy một Spark application thật, ghi `range(1000)` thành Parquet trên HDFS và đọc lại:

```bash
./scripts/verify_cluster.sh
```

## 8. Phase 2 – Data Pipeline

### Upload

```bash
./scripts/upload_hdfs.sh
docker compose exec namenode hdfs dfs -ls -h /financial/raw
```

Sau khi upload, Spark chỉ dùng `hdfs:///financial/raw` làm nguồn chính. Local filesystem không thay thế HDFS.

### Cleaning và Parquet

```bash
./scripts/run_phase2.sh
```

Pipeline thực hiện:

1. Áp dụng schema tường minh.
2. Kiểm tra null, blank và transaction ID trùng.
3. Chuẩn hóa timestamp, amount, ZIP, MCC và errors.
4. Chuyển `Yes/No` thành `class = 1/0`.
5. Join transaction và label theo `transaction_id`.
6. Ghi Parquet partition theo năm.
7. Đọc lại và đối chiếu row count.

Output:

```text
hdfs:///financial/processed/transactions
hdfs:///financial/processed/labeled_transactions
output/results/phase2_summary.json
```

Kết quả lần chạy hiện tại:

- 13.305.915 giao dịch sạch.
- 8.914.963 giao dịch có nhãn.
- 13.332 Fraud, tương đương khoảng 0,1495% dữ liệu có nhãn.
- Không có duplicate hoặc dòng không hợp lệ bị loại trong lần chạy này.

## 9. Phase 3 – Machine Learning

```bash
./scripts/run_phase3.sh
./scripts/verify_phase3.sh
```

Pipeline:

```text
Labeled Parquet
    ↓ deterministic hash split, seed 42
Train 80%                              Test 20%
    ↓ fit preprocessing trên Train        └── giữ nguyên phân bố thực
    ├── LR None
    ├── LR Class Weight
    ├── RF None
    ├── RF Class Weight
    ├── RF Undersampling, chỉ Train
    └── Spark features → sklearn Isolation Forest
```

Feature vector không chứa `class`, transaction/customer/card/merchant ID. StringIndexer, OneHotEncoder và StandardScaler chỉ được fit trên Train để tránh data leakage.

Mặc định LR/RF dùng compute cap khoảng 500.000 Train rows, chọn class-neutral bằng hash để phù hợp máy cá nhân. Test Set đầy đủ không bị sampling. Có thể tăng giới hạn nếu Docker có nhiều RAM/CPU:

```powershell
.\scripts\run_phase3.ps1 --max-supervised-train-rows 1000000 --rf-num-trees 50
```

Model được lưu tại:

```text
/financial/models/preprocessor
/financial/models/logistic_regression/none
/financial/models/logistic_regression/class_weight
/financial/models/random_forest/none
/financial/models/random_forest/class_weight
/financial/models/random_forest/undersampling
/financial/models/isolation_forest/model.joblib
/financial/models/metadata/phase3_summary.json
```

Isolation Forest fit local trên tối đa 200.000 dòng đã được Spark tạo feature. `Class` không tham gia fit. Khi Phase 4 chạy inference, artifact sklearn được Spark phân phối tới worker và xử lý Test theo batch, không `.toPandas()` toàn bộ Test Set.

## 10. Phase 4 – Evaluation

```bash
./scripts/run_phase4.sh
./scripts/verify_phase4.sh
```

Mọi model được đánh giá trên cùng 1.782.734 dòng Test không sampling. Metric chính tính riêng cho Fraud (`class = 1`):

- TP, TN, FP, FN.
- Precision, Recall và F1.
- PR-AUC.
- Threshold `0.2, 0.3, 0.4, 0.5, 0.6, 0.7` cho model có probability.
- Accuracy chỉ được ghi để tham khảo, không dùng làm tiêu chí duy nhất.

Output local nhỏ, phù hợp để đọc bằng pandas hoặc đưa vào biểu đồ:

```text
output/results/phase4_summary.json
output/results/model_comparison.csv
output/results/imbalance_experiments.csv
output/results/confusion_matrices.csv
output/results/threshold_analysis.csv
output/results/rf_feature_importance.csv
```

Prediction đầy đủ được lưu dưới dạng Parquet:

```text
/financial/predictions/lr_none
/financial/predictions/lr_class_weight
/financial/predictions/rf_none
/financial/predictions/rf_class_weight
/financial/predictions/rf_undersampling
/financial/predictions/isolation_forest
/financial/predictions/metadata
```

Schema prediction:

```text
transaction_id, transaction_time, amount,
fraud_probability hoặc anomaly_score,
prediction, status, actual_class, model
```

### Kết quả thực nghiệm hiện tại tại threshold 0,5

| Model | Imbalance | Precision | Recall | F1 | PR-AUC |
|---|---|---:|---:|---:|---:|
| Logistic Regression | None | 0,7050 | 0,2141 | 0,3285 | 0,2277 |
| Logistic Regression | Class Weight | 0,0323 | 0,9249 | 0,0624 | 0,2250 |
| Random Forest | None | 1,0000 | 0,0015 | 0,0030 | 0,5319 |
| Random Forest | Class Weight | 0,0141 | 0,8892 | 0,0278 | 0,0893 |
| Random Forest | Undersampling | 0,3464 | 0,2394 | 0,2831 | 0,2492 |
| Isolation Forest | Unsupervised boundary | 0,0030 | 0,7743 | 0,0060 | 0,0085 |

Diễn giải:

- Class weight tăng mạnh Recall nhưng tạo nhiều False Positive.
- RF không xử lý imbalance xếp hạng khá tốt theo PR-AUC, nhưng threshold mặc định `0,5` quá bảo thủ và chỉ bắt được 4/2.690 fraud.
- Threshold thấp thường tăng Recall và giảm Precision.
- Không tự động chọn threshold “tối ưu” khi chưa biết chi phí nghiệp vụ của False Negative và False Positive.
- Random Forest feature importance không đồng nghĩa với quan hệ nhân quả.

## 11. Phase 5 – Visualization, Demo và Prediction mới

Chạy toàn bộ Phase 5:

```bash
./scripts/run_phase5.sh
./scripts/verify_phase5.sh
```

Windows:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_phase5.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify_phase5.ps1
```

Phase 5 tạo 10 hình trong `output/figures/`:

```text
01_class_distribution.png
02_amount_distribution.png
03_fraud_amount_distribution.png
04_confusion_matrix.png
05_metric_comparison.png
06_precision_recall_curve.png
07_rf_feature_importance.png
08_probability_distribution.png
09_threshold_analysis.png
10_top_suspicious_transactions.png
```

Biểu đồ dùng kết quả thực nghiệm thật. Amount histogram, probability histogram và PR curve được aggregate bằng Spark trước; không `.toPandas()` toàn bộ dataset. PR curve là xấp xỉ phân tán với 200 score bins.

Top 20 giao dịch được gắn cờ bởi model demo `rf_undersampling` được lưu tại:

```text
hdfs:///financial/predictions/suspicious_transactions
output/results/top_suspicious_transactions.csv
```

Model này được chọn để minh họa, không được tuyên bố là lựa chọn tối ưu cho nghiệp vụ.

### Dự đoán dữ liệu mới

File mẫu [new_transactions.csv](data/sample/new_transactions.csv) không có cột `Class` và không chứa thông tin thẻ thật. Script thực hiện đúng luồng:

```text
new_transactions.csv → HDFS → Spark feature engineering
→ load preprocessor/model từ HDFS → risk score → NORMAL/NEEDS_REVIEW
```

Chạy riêng:

```bash
./scripts/predict_new_transactions.sh
```

Windows:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\predict_new_transactions.ps1
```

Mặc định dùng RF Undersampling và threshold `0,5`. Có thể chọn model/threshold khác mà không train lại:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\predict_new_transactions.ps1 --model lr_class_weight --threshold 0.7
```

Output:

```text
hdfs:///financial/raw/new_transactions.csv
hdfs:///financial/predictions/new_transactions
output/results/new_transactions_predictions.csv
output/results/new_prediction_summary.json
```

Lần chạy kiểm thử hiện tại đọc 10 giao dịch, dùng model đã lưu, không train lại và gắn cờ 1 giao dịch `NEEDS_REVIEW`.

## 12. Dùng Jupyter để học và thuyết trình

Mở http://localhost:8888, nhập token `fraud-demo`, rồi chạy notebook theo thứ tự:

1. `00_infrastructure_check.ipynb`
2. `01_data_exploration.ipynb`
3. `02_data_cleaning.ipynb`
4. `03_feature_engineering.ipynb`
5. `04_logistic_regression.ipynb`
6. `05_random_forest.ipynb`
7. `06_isolation_forest.ipynb`
8. `07_model_evaluation.ipynb`
9. `08_visualization.ipynb`

Khi demo cuối kỳ, chỉ cần mở `00_full_demo.ipynb`; notebook này load toàn bộ artifact đã sinh và đi theo kịch bản 7–10 phút mà không chạy lại training.

Các notebook từ Phase 3 trở đi ưu tiên load artifact đã lưu thay vì train lại. Chỉ aggregate nhỏ mới được đưa về pandas.

## 13. Kiểm tra HDFS và output

```bash
docker compose exec namenode hdfs dfs -ls -R /financial
docker compose exec namenode hdfs dfs -du -h -s /financial/models
docker compose exec namenode hdfs dfs -du -h -s /financial/predictions
docker compose exec namenode hdfs fsck /financial/models
docker compose exec namenode hdfs fsck /financial/predictions
docker compose exec namenode hdfs dfsadmin -report
```

Xem thử các giao dịch được gắn cờ bằng PySpark trong notebook:

```python
pred = spark.read.parquet("hdfs:///financial/predictions/rf_undersampling")
pred.where("prediction = 1").orderBy(
    "fraud_probability", ascending=False
).show(20, truncate=False)
```

## 14. Dừng, restart và reset

Dừng cluster nhưng giữ nguyên HDFS:

```bash
./scripts/stop.sh
```

Windows:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stop.ps1
```

Khởi động lại rồi dùng model cũ:

```bash
docker compose up -d --wait
./scripts/verify_phase3.sh
```

Chỉ khi muốn xóa toàn bộ HDFS và làm lại từ đầu mới dùng:

```bash
docker compose down -v
```

> `docker compose down -v` xóa không thể hoàn tác các named volume HDFS của project.

## 15. Kịch bản demo 7–10 phút

1. `docker compose ps`: chứng minh 7 service healthy.
2. NameNode UI: mở `/financial/raw`, `/processed`, `/models`, `/predictions`.
3. Spark Master UI: chứng minh có 2 worker.
4. Chạy `00_full_demo.ipynb`: class imbalance khoảng 0,15% Fraud.
5. Trình bày Parquet, split và feature engineering không leakage.
6. Hiển thị model comparison, confusion matrix và threshold table.
7. Đọc `/financial/predictions/suspicious_transactions`, hiển thị `NEEDS_REVIEW`.
8. Đọc `/financial/predictions/new_transactions`, chứng minh tái sử dụng model.
9. Nhấn mạnh Precision–Recall trade-off và model không kết luận gian lận.

Kịch bản lời nói chi tiết nằm trong [docs/DEMO_SCRIPT.md](docs/DEMO_SCRIPT.md). Nội dung báo cáo sáu chương nằm trong [docs/REPORT.md](docs/REPORT.md).

## 16. Troubleshooting

- `permission denied ... dockerDesktopLinuxEngine`: mở Docker Desktop, đợi engine chạy xong rồi mở lại terminal; trên Windows kiểm tra user thuộc group `docker-users`.
- `running scripts is disabled`: dùng `powershell -NoProfile -ExecutionPolicy Bypass -File ...`; cờ chỉ có hiệu lực với process hiện tại.
- Port `8080`, `8081`, `8082`, `8888` hoặc `9870` đã được dùng: dừng ứng dụng chiếm port hoặc đổi phần bên trái của mapping trong `compose.yaml`.
- Container `unhealthy`: chạy `docker compose ps` và `docker compose logs --tail=150 <service>` trước khi xóa volume.
- Không đủ 2 DataNode: `docker compose restart datanode-1 datanode-2`, sau đó chạy lại `verify_cluster`.
- `Path does not exist` ở Phase 3/4/5: phase trước chưa chạy hoặc HDFS volume đã bị xóa; kiểm tra bằng `hdfs dfs -ls -R /financial`.
- Docker hết RAM: đóng workload khác, tăng RAM Docker Desktop hoặc giữ compute cap mặc định của Phase 3.
- Muốn đổi tham số: chạy script với các argument của Python app; dùng `--help` sau đường dẫn script để xem lựa chọn.

Lệnh lấy log nhanh:

```bash
docker compose logs --tail=150 namenode datanode-1 datanode-2
docker compose logs --tail=150 spark-master spark-worker-1 spark-worker-2
docker compose logs --tail=150 jupyter
docker compose config --quiet
```

## 17. Trạng thái hoàn thành và hướng mở rộng

Pipeline yêu cầu đã hoàn thành end-to-end: raw data → HDFS → cleaning/Parquet → feature engineering → imbalance experiments → model → evaluation → prediction/visualization → save/load model → predict dữ liệu mới.

Các hướng mở rộng sau đồ án:

- Spark Structured Streaming hoặc Kafka cho near-real-time scoring.
- Feature lịch sử theo cửa sổ thời gian và hành vi tài khoản.
- Probability calibration và threshold theo chi phí nghiệp vụ.
- Explainability, drift monitoring, model registry và analyst feedback loop.

Nguyên tắc tiếp tục giữ nguyên: dữ liệu lớn xử lý bằng Spark; chỉ aggregate hoặc sample nhỏ mới đưa sang pandas/matplotlib; không sampling Test Set; không hard-code metric; không đưa `Class` vào feature.
