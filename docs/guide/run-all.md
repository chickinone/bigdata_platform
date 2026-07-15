# Hướng dẫn chạy & sử dụng toàn bộ platform

> Vận hành đầu-cuối bằng Docker: khởi động, mở link nào, đăng ký connector, chạy job, kiểm tra dữ
> liệu ở từng tầng, dừng/reset. Đây là guide chính; các file `guide/*.md` khác đi sâu theo công cụ.
> Cập nhật lần cuối: 2026-07-15.

Mọi lệnh chạy ở **thư mục gốc repo** (nơi có `docker-compose.yml`).

> **Windows:** dùng `curl.exe` thay `curl` (PowerShell alias `curl` sang `Invoke-WebRequest`, cú pháp
> khác hẳn). Đường dẫn container (`/opt/...`) có thể bị Git Bash mangling — chạy `docker exec` trong
> **PowerShell**, hoặc thêm `//` (vd `//opt`).

---

## 1. Điều kiện tiên quyết

| Yêu cầu | Tối thiểu |
|---|---|
| Docker Desktop + Compose v2 | Bắt buộc |
| RAM cấp cho Docker | **10–12 GB** (stack có 21 container) |
| Disk trống | 30 GB |
| CPU | 4 core |

Tạo `.env` ở thư mục gốc trước khi làm gì khác — compose tham chiếu **48 biến** và **không có giá trị
mặc định**. Danh sách đầy đủ: [`../infra/infra.md`](../infra/infra.md) §4.

---

## 2. Khởi động stack

```bash
docker compose up -d --build      # lần đầu khá lâu (build 4 image, tải ~15 image)
docker compose ps                 # kỳ vọng: 19 service Up (generator không chạy - profile riêng)
```

| Mở link | Cổng | Dùng để | Đăng nhập |
|---|---|---|---|
| **Kafka UI** | http://localhost:8080 | Topic, message, trạng thái connector | — |
| **Flink Web UI** | http://localhost:8082 | Job, checkpoint, backpressure | — |
| **Grafana** | http://localhost:3000 | Dashboard realtime từ ClickHouse | `.env`: `GRAFANA_ADMIN_*` |
| **Kibana** | http://localhost:5601 | Điều tra fraud/failed transaction | — |
| **MinIO Console** | http://localhost:9001 | Bucket Bronze/Silver/Gold/Iceberg | `.env`: `MINIO_ROOT_*` |
| **Spark Master** | http://localhost:8090 | Batch job | — |
| **Trino** | http://localhost:8085 | Query liên nguồn | — |
| Kafka Connect REST | http://localhost:8083 | Đăng ký/kiểm tra connector | — |
| Schema Registry | http://localhost:8081 | Avro schema | — |
| ClickHouse HTTP | http://localhost:8123 | Query OLAP | `.env`: `CLICKHOUSE_*` |
| Elasticsearch | http://localhost:9200 | Search API | — |
| Iceberg REST | http://localhost:8181 | Catalog Iceberg | — |

---

## 3. Bootstrap thủ công (⚠️ bắt buộc — không tự động)

Hai việc **không** được compose làm hộ. Bỏ qua sẽ dẫn tới "chạy mà không ra dữ liệu".

### 3.1 Tạo bucket lake

`minio-init` **chỉ** tạo `flink-checkpoints` và `flink-savepoints`. Các bucket lake phải tạo tay,
nếu không S3 sink và Spark job sẽ fail:

```bash
docker exec bigdata-minio-init sh -c '
  mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD";
  mc mb -p local/data-lake-bronze local/data-lake-silver local/data-lake-gold local/data-lake-iceberg'
```

Container `minio-init` thoát sau khi chạy xong, nên có thể dùng `mc` trong container `minio`, hoặc
đơn giản là **tạo bằng MinIO Console** ở http://localhost:9001.

### 3.2 Khởi tạo schema ClickHouse

`clickhouse/init/` **không** được mount vào entrypoint, nên schema không tự chạy:

```bash
docker exec -i bigdata-clickhouse clickhouse-client --user admin --password "$CLICKHOUSE_PASSWORD" \
  --multiquery < clickhouse/init/01_schema.sql
docker exec -i bigdata-clickhouse clickhouse-client --user admin --password "$CLICKHOUSE_PASSWORD" \
  --multiquery < clickhouse/init/02_kafka_consumers.sql
```

PowerShell:
```powershell
Get-Content .\clickhouse\init\01_schema.sql | docker exec -i bigdata-clickhouse clickhouse-client --user admin --password $env:CLICKHOUSE_PASSWORD --multiquery
Get-Content .\clickhouse\init\02_kafka_consumers.sql | docker exec -i bigdata-clickhouse clickhouse-client --user admin --password $env:CLICKHOUSE_PASSWORD --multiquery
```

Kiểm tra — kỳ vọng 12 bảng (4 target + 4 kafka + 4 MV):
```bash
docker exec bigdata-clickhouse clickhouse-client --user admin --password "$CLICKHOUSE_PASSWORD" \
  --query "SHOW TABLES FROM metrics"
```

> **Thứ tự quan trọng:** chạy `02_kafka_consumers.sql` **trước khi** có message trong topic `metrics.*`
> thì Kafka engine bắt đầu đọc từ đầu. Chi tiết: [`clickhouse-grafana.md`](clickhouse-grafana.md).

---

## 4. Đăng ký connector

Đầy đủ ở [`cdc-and-connectors.md`](cdc-and-connectors.md). Bản rút gọn:

```bash
# 1. Debezium source — phải chạy TRƯỚC (tạo topic CDC)
curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json" \
  --data-binary "@debezium/postgres-connector.json"

# 2. ES sinks (5 cái)
for f in customers accounts transactions transfers fraud-alerts; do
  curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json" \
    --data-binary "@kafka-connect/es-sinks/es-sink-$f.json"
done

# 3. S3 sink → Bronze
curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json" \
  --data-binary "@kafka-connect/s3-sinks/s3-sink-cdc.json"
```

Kiểm tra tất cả `RUNNING`:
```bash
curl.exe http://localhost:8083/connectors?expand=status
```

---

## 5. Sinh dữ liệu

Generator nằm trong profile `generator` nên **không** tự chạy:

```bash
docker compose --profile generator up generator
```

Mặc định: 150 RPS nền, burst tới 800 RPS, chạy 900 giây, ~5% giao dịch fail, ~20% là transfer. Chỉnh
qua `.env` — xem [`../infra/infra.md`](../infra/infra.md) §4.

> Job fraud dùng `scan.startup.mode = 'latest-offset'`. **Submit job trước, rồi mới chạy generator**,
> nếu không sẽ không có alert nào cho dữ liệu đã sinh.

---

## 6. Chạy job Flink

Chi tiết: [`flink-jobs.md`](flink-jobs.md).

```bash
# Lane 1 — 4 metric trong 1 job. CHỈ chạy file này.
docker exec -it bigdata-flink-jobmanager flink run -py /opt/flink/jobs/lane1_dashboard.py

# Lane 3 — fraud detection
docker exec -it bigdata-flink-jobmanager flink run -py /opt/flink/jobs/lane3_fraud_detection.py

docker exec -it bigdata-flink-jobmanager flink list
```

> ⚠️ **Không** chạy `lane1_timeseries.py`, `lane1_kpi.py`, `lane1_breakdown.py`, `lane1_topn.py`.
> Logic của chúng đã nằm trong `lane1_dashboard.py`; chạy song song sẽ ghi trùng vào topic metric.
> Xem [ADR-0006](../decisions/0006-one-flink-job-per-lane-statement-set.md).

---

## 7. Chạy job Spark

Chi tiết: [`spark-lakehouse.md`](spark-lakehouse.md). Phải theo **đúng thứ tự** — không có orchestrator:

```bash
# Bronze → Silver (chờ S3 sink đổ đủ dữ liệu đã)
docker exec -it bigdata-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/spark-jobs/enrich_transactions.py

# Silver → Gold
docker exec -it bigdata-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/spark-jobs/build_gold_layer.py

# Silver → Iceberg (+ demo time travel)
docker exec -it bigdata-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/spark-jobs/silver_to_iceberg.py
```

---

## 8. Kiểm tra dữ liệu ở từng tầng

Đi ngược từ nguồn ra đích — chỗ nào đứt sẽ lộ ngay.

```bash
# 1. Postgres có dữ liệu?
docker exec -it bigdata-source-postgres psql -U admin -d bankdb -c "SELECT COUNT(*) FROM transactions"

# 2. CDC có chảy? (kỳ vọng 4 topic bankdb.public.* + metrics.* + fraud-alerts)
docker exec -it bigdata-kafka kafka-topics --bootstrap-server kafka:9092 --list

# 3. Message CDC đúng dạng?
docker exec -it bigdata-kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic bankdb.public.transactions --from-beginning --max-messages 5

# 4. Flink có ghi metric?
docker exec -it bigdata-kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic metrics.kpi --from-beginning --max-messages 3

# 5. ClickHouse có nhận?
docker exec -it bigdata-clickhouse clickhouse-client --user admin --password "$CLICKHOUSE_PASSWORD" \
  --query "SELECT * FROM metrics.kpi ORDER BY window_end DESC LIMIT 5"

# 6. Có alert fraud?
docker exec -it bigdata-kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic fraud-alerts --from-beginning

# 7. ES có index?
curl.exe http://localhost:9200/_cat/indices?v

# 8. Bronze có file Parquet? → MinIO Console http://localhost:9001
```

---

## 9. Dừng & reset

```bash
docker compose down                 # dừng, GIỮ dữ liệu (volume còn nguyên)
docker compose down -v              # dừng + XÓA SẠCH mọi volume
```

`down -v` xoá toàn bộ: dữ liệu Postgres, offset Kafka, bảng ClickHouse, bucket MinIO, index ES,
dashboard Grafana. Sau đó phải làm lại từ **§3** (bootstrap) — kể cả bucket và schema ClickHouse.

Reset một phần, giữ nguồn:
```bash
docker compose restart jobmanager taskmanager-1 taskmanager-2   # chỉ Flink
curl.exe -X DELETE http://localhost:8083/connectors/postgres-source-connector   # chỉ connector
```

> Xoá Debezium connector **không** xoá replication slot ở Postgres. Slot mồ côi sẽ giữ WAL lại và làm
> đầy đĩa. Dọn: xem [`cdc-and-connectors.md`](cdc-and-connectors.md) §5.

---

## 10. Troubleshooting

| Triệu chứng | Nguyên nhân thường gặp | Xử lý |
|---|---|---|
| Grafana rỗng, ClickHouse không có bảng | Chưa chạy §3.2 | Chạy 2 file SQL init |
| S3 sink `FAILED`, lỗi NoSuchBucket | Chưa tạo bucket (§3.1) | Tạo bucket rồi `PUT .../restart` |
| Không có topic CDC | Debezium chưa đăng ký, hoặc publication thiếu | `curl .../connectors/postgres-source-connector/status` |
| Không có alert fraud | Job submit **sau** khi generator chạy xong (`latest-offset`) | Submit job trước, chạy lại generator |
| Metric trùng/gấp đôi | Đang chạy cả `lane1_dashboard` lẫn `lane1_*` rời | `flink list` → `flink cancel <id>` job thừa |
| ClickHouse có bảng nhưng rỗng | MV tạo **sau** khi message đã trôi qua; hoặc lệch cột → MV im lặng bỏ | Kiểm tra `SELECT * FROM metrics.timeseries_kafka` |
| Trino lỗi mount `jvm.config` | `trino/etc/jvm.config` trên host bị tạo thành **folder** | Xoá, tạo lại đúng dạng **file**, `docker compose up -d trino` |
| Connector "already exists" | Đăng ký lại connector cũ | `curl -X DELETE .../connectors/<tên>` rồi POST lại |
| Avro không decode được | Schema Registry chưa sẵn sàng, hoặc sai converter | Kiểm tra `curl http://localhost:8081/subjects` |
| Spark OOM / treo | Docker thiếu RAM; hoặc join raw CDC chưa dedup | Tăng RAM lên 12 GB; giảm `TARGET_RPS`/`DURATION_SEC` |
| `dlq-processor` không ghi gì | **Đúng như hiện trạng** — chưa connector nào bật DLQ | [`dlq-and-notifier.md`](dlq-and-notifier.md) |
