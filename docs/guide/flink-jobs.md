# Job Flink — submit, theo dõi, huỷ

> Cách chạy 2 job streaming, đọc Web UI, và vì sao **chỉ** nên chạy `lane1_dashboard.py`.
> Thiết kế: [`../architecture/BDP-streaming-lanes.md`](../architecture/BDP-streaming-lanes.md).
> Cập nhật lần cuối: 2026-07-15.

---

## 1. Chỉ chạy hai file này

Thư mục [`flink/jobs/`](../../flink/jobs/) có 6 file, nhưng **chỉ 2 file nên chạy**:

| File | Chạy? | Vì sao |
|---|---|---|
| `lane1_dashboard.py` | ✅ **Có** | Sinh cả 4 metric trong 1 job qua `StatementSet`. |
| `lane3_fraud_detection.py` | ✅ **Có** | Fraud detection. |
| `lane1_timeseries.py` | ❌ Không | **Di sản** — đã gộp vào `lane1_dashboard.py`. |
| `lane1_kpi.py` | ❌ Không | Di sản. |
| `lane1_breakdown.py` | ❌ Không | Di sản. |
| `lane1_topn.py` | ❌ Không | Di sản. |

Chạy file di sản song song với `lane1_dashboard` → **hai job cùng ghi vào một topic metric**, ClickHouse
nhận số liệu gấp đôi. Vì bảng dùng `ReplacingMergeTree(inserted_at)` chứ không phải khoá nghiệp vụ, dữ
liệu trùng **không** bị gộp một cách đáng tin cậy. Xem
[ADR-0006](../decisions/0006-one-flink-job-per-lane-statement-set.md).

---

## 2. Submit

```bash
# Lane 1 — 4 metric, một job
docker exec -it bigdata-flink-jobmanager flink run -py /opt/flink/jobs/lane1_dashboard.py

# Lane 3 — fraud
docker exec -it bigdata-flink-jobmanager flink run -py /opt/flink/jobs/lane3_fraud_detection.py
```

Job chạy **foreground** cho tới khi bị huỷ. Thêm `-d` để detach:
```bash
docker exec -it bigdata-flink-jobmanager flink run -d -py /opt/flink/jobs/lane1_dashboard.py
```

> **Thứ tự với generator:** Lane 3 dùng `scan.startup.mode='latest-offset'` → chỉ thấy dữ liệu đến
> **sau** khi submit. Submit job **trước**, rồi mới chạy generator. Lane 1 thì `earliest-offset` nên
> luôn tính lại từ đầu topic, không phụ thuộc thứ tự.

`flink/jobs/` được mount từ host vào cả JobManager lẫn TaskManager, nên sửa file `.py` trên máy là
có hiệu lực ngay ở lần submit sau — không cần rebuild image.

---

## 3. Theo dõi

**Web UI:** http://localhost:8082

```bash
docker exec -it bigdata-flink-jobmanager flink list              # job đang chạy
docker exec -it bigdata-flink-jobmanager flink list -a           # cả job đã kết thúc
docker logs -f bigdata-flink-taskmanager-1                       # log thực thi (print sink ra đây)
```

Cần nhìn gì trên UI:

| Chỗ | Ý nghĩa |
|---|---|
| **Checkpoints** → History | Phải thấy checkpoint mỗi 30 giây. Fail liên tục = state quá lớn hoặc MinIO không tới được. |
| **Checkpoints** → Size | Job Lane 1 **phình dần suốt ngày** rồi rơi về 0 lúc nửa đêm (do `COUNT(DISTINCT)` trên CUMULATE 1 ngày). Đúng thiết kế. |
| **Backpressure** | Đỏ ở source = sink chậm hơn nguồn. |
| **Watermarks** | Đứng yên = có partition im lặng. `table.exec.source.idle-timeout=5000ms` đã xử lý phần lớn. |
| **Exceptions** | Chỗ đầu tiên nên xem khi job restart vòng lặp. |

Cluster có **4 slot** (2 TaskManager × 2 slot). Cả 2 job đều `parallelism = 1`, nên còn thừa slot.

---

## 4. Huỷ & savepoint

```bash
docker exec -it bigdata-flink-jobmanager flink cancel <job-id>

# Dừng có savepoint (giữ state để chạy tiếp sau)
docker exec -it bigdata-flink-jobmanager flink stop --savepointPath s3a://flink-savepoints/savepoints <job-id>

# Khôi phục từ savepoint
docker exec -it bigdata-flink-jobmanager flink run -s <duong-dan-savepoint> -py /opt/flink/jobs/lane1_dashboard.py
```

Checkpoint ở `s3a://flink-checkpoints/checkpoints`, savepoint ở `s3a://flink-savepoints/savepoints` —
đây là 2 bucket **duy nhất** được `minio-init` tạo tự động.

---

## 5. Xác minh output

```bash
# Lane 1 ghi metric?
docker exec -it bigdata-kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic metrics.kpi --from-beginning --max-messages 3

# Lane 3 ghi alert?
docker exec -it bigdata-kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic fraud-alerts --from-beginning
```

`metrics.timeseries` có dữ liệu sau **1 phút** (TUMBLE 1 phút). Ba metric còn lại dùng CUMULATE 5 phút
nên phải chờ tới mốc 5 phút đầu tiên.

Alert cần đủ ngưỡng: Velocity là **>5 giao dịch/phút/account**, Failed Storm là **≥15 giao dịch failed
trong 5 phút/account**. Ở 150 RPS trải trên nhiều account, Velocity xuất hiện đều; Failed Storm hiếm
hơn (chỉ ~5% giao dịch fail). Muốn ép ra alert, tăng `PEAK_RPS` hoặc `PROB_FAILURE` trong `.env`.

---

## 6. Vấn đề thường gặp

| Triệu chứng | Nguyên nhân | Xử lý |
|---|---|---|
| Không có alert nào | Job submit **sau** khi generator xong (`latest-offset`) | Submit trước, chạy lại generator |
| Metric gấp đôi | Đang chạy cả `lane1_dashboard` lẫn file di sản | `flink list` → `flink cancel` job thừa |
| Log TaskManager phình rất nhanh | `lane3` còn `ds.print("LANE3-RAW")` — in **mọi** transaction | Gỡ dòng print rồi submit lại |
| Checkpoint fail liên tục | Không tới được MinIO, hoặc thiếu bucket checkpoint | Kiểm tra `bigdata-minio-init` đã chạy xong |
| `ClassNotFound` connector Kafka | JAR không có trong `/opt/flink/jobs/jars` | Kiểm tra thư mục `flink/jobs/jars/` trên host |
| Job restart vòng lặp | Xem tab **Exceptions** trên UI | Thường là lỗi decode Avro → kiểm tra Schema Registry |
| Watermark không tiến | Topic không có dữ liệu mới | Chạy generator |
