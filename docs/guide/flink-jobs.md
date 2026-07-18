# Job Flink — submit, theo dõi, huỷ

> Cách submit 2 runner streaming (sinh từ metadata), đọc Web UI, huỷ & savepoint.
> Thiết kế: [`../architecture/BDP-streaming-lanes.md`](../architecture/BDP-streaming-lanes.md).
> Cập nhật lần cuối: 2026-07-18.

---

## 1. Hai runner — SINH từ metadata

Thư mục [`flink/jobs/`](../../flink/jobs/) có 2 runner; SQL/tham số của chúng **sinh** từ
[`metadata/pipelines/stream/`](../../metadata/pipelines/stream/) ([ADR-0023](../decisions/0023-flink-metric-runner-declarative.md)):

| File | Vì sao |
|---|---|
| `metric_runner.py` | Thực thi job plan sinh sẵn — 4 metric trong 1 `StatementSet`. |
| `fraud_runner.py` | Fraud: source DDL + tham số sinh, detector có state giữ là code. |

> **Đã xoá** `lane1_{timeseries,kpi,breakdown,topn}.py`, `lane1_dashboard.py`, `lane3_fraud_detection.py`
> — thay bằng 2 runner sinh ở trên. Xem [ADR-0006](../decisions/0006-one-flink-job-per-lane-statement-set.md)
> (một job/lane) và [ADR-0023](../decisions/0023-flink-metric-runner-declarative.md) (sinh từ spec).

---

## 2. Submit — qua deployer

Cách đúng: **deployer** sinh config từ metadata rồi submit cả hai runner.

```bash
python -m dataplatform.deployers.flink_metrics plan    # xem sẽ submit gì (không đụng Flink)
python -m dataplatform.deployers.flink_metrics apply   # submit metric_runner + fraud_runner
```

> Deployer `apply` submit MỚI, **không** huỷ job cũ — nếu đang chạy thì `flink cancel` trước để tránh
> hai bản cùng ghi. Cần submit tay một runner (debug) thì vẫn được:
> ```bash
> docker exec -it bigdata-flink-jobmanager flink run -d -py /opt/flink/jobs/metric_runner.py
> ```

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
docker exec -it bigdata-flink-jobmanager flink run -s <duong-dan-savepoint> -py /opt/flink/jobs/metric_runner.py
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
| Metric gấp đôi | Đang chạy `metric_runner` hai lần (deployer `apply` không huỷ job cũ) | `flink list` → `flink cancel` job thừa |
| ~~Log TaskManager phình vì `ds.print`~~ | Đã bỏ trong `fraud_runner.py` (ADR-0023) | — |
| Checkpoint fail liên tục | Không tới được MinIO, hoặc thiếu bucket checkpoint | Kiểm tra `bigdata-minio-init` đã chạy xong |
| `ClassNotFound` connector Kafka | JAR không có trong `/opt/flink/jobs/jars` | Kiểm tra thư mục `flink/jobs/jars/` trên host |
| Job restart vòng lặp | Xem tab **Exceptions** trên UI | Thường là lỗi decode Avro → kiểm tra Schema Registry |
| Watermark không tiến | Topic không có dữ liệu mới | Chạy generator |
