# Job Spark — Bronze → Silver → Gold → Iceberg

> Chạy 3 batch job đúng thứ tự, packages cần thiết, và cách kiểm tra output.
> Thiết kế: [`../architecture/BDP-lakehouse-medallion.md`](../architecture/BDP-lakehouse-medallion.md).
> Cập nhật lần cuối: 2026-07-15.

---

## 1. Điều kiện tiên quyết

Trước khi chạy job Spark nào:

1. **Bucket đã tồn tại:** `data-lake-{bronze,silver,gold,iceberg}` — `minio-init` **không** tạo chúng.
   Xem [`run-all.md`](run-all.md) §3.1.
2. **Bronze đã có dữ liệu:** `s3-sink-cdc` phải chạy đủ lâu để flush Parquet (1000 record **hoặc**
   5 phút, cái nào tới trước). Kiểm tra ở MinIO Console http://localhost:9001.

```bash
# Bronze đã có file chưa?
docker exec bigdata-minio mc ls -r local/data-lake-bronze/topics/ | head
```

---

## 2. Thứ tự phụ thuộc

**Không có orchestrator** — không Airflow, không cron. Phải chạy tay theo đúng thứ tự:

```text
s3-sink-cdc (liên tục)
        ↓  cần đủ dữ liệu Bronze
enrich_transactions.py   →   Silver
        ↓                       ↓
build_gold_layer.py      silver_to_iceberg.py
   (cần Silver)             (cần Silver)
```

Chạy `build_gold_layer.py` khi chưa có Silver → fail ngay ở bước đọc Parquet với `Path does not exist`.

---

## 3. Ba job

### 3.1 Bronze → Silver

```bash
docker exec -it bigdata-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/spark-jobs/enrich_transactions.py
```

Đọc Bronze `customers`/`accounts`/`transactions` → dedup về current state theo `updated_at` → join →
ghi `s3a://data-lake-silver/enriched_transactions/` partition `year/month/day`.

Job in số dòng ở từng bước — `Accounts (raw events)` so với `Accounts (current state)` cho thấy dedup
đã nén bao nhiêu.

> `mode("overwrite")` → **ghi đè toàn bộ Silver mỗi lần chạy** (full refresh). Không incremental.

### 3.2 Silver → Gold

```bash
docker exec -it bigdata-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/spark-jobs/build_gold_layer.py
```

Sinh 3 bảng dưới `s3a://data-lake-gold/`: `daily_transaction_summary`, `customer_lifetime_metrics`,
`high_risk_transactions`.

### 3.3 Silver → Iceberg

```bash
docker exec -it bigdata-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/spark-jobs/silver_to_iceberg.py
```

Chú ý `--packages` **khác** hai job trên: có thêm `iceberg-spark-runtime`. Thiếu nó → `ClassNotFoundException`.

Job này `DROP TABLE ... PURGE` rồi tạo lại `lakehouse.silver.enriched_transactions`, append 1000 row để
có snapshot thứ hai, rồi demo time travel. **Là job trình diễn** — mọi lịch sử snapshot cũ mất sạch
mỗi lần chạy.

---

## 4. Vì sao cần `--packages`

Image `apache/spark:3.5.0` **không** kèm JAR S3A hay Iceberg. `--packages` tải chúng từ Maven lúc
submit.

| Package | Cho việc gì |
|---|---|
| `org.apache.hadoop:hadoop-aws:3.3.4` | `s3a://` filesystem |
| `com.amazonaws:aws-java-sdk-bundle:1.12.262` | SDK mà hadoop-aws phụ thuộc — **phải khớp phiên bản** |
| `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0` | Iceberg (chỉ job §3.3) |

> **Lần đầu chạy sẽ lâu** — Spark tải hàng chục MB JAR từ Maven Central về `~/.ivy2` trong container.
> Cần mạng. `docker compose down` **không** xoá cache này (nằm trong lớp container), nhưng
> `docker compose down -v` + rebuild thì phải tải lại.

---

## 5. Theo dõi

**Spark Master UI:** http://localhost:8090 · **Worker UI:** http://localhost:8091

Worker được cấp **2 core / 2 GB** ([`docker-compose.yml`](../../docker-compose.yml)). Đó là trần của
mọi job — không có worker thứ hai.

```bash
docker logs -f bigdata-spark-master
docker logs -f bigdata-spark-worker    # log executor thật nằm ở đây
```

---

## 6. Kiểm tra output

```bash
# Silver
docker exec bigdata-minio mc ls -r local/data-lake-silver/enriched_transactions/ | head

# Gold
docker exec bigdata-minio mc ls local/data-lake-gold/

# Iceberg — qua Trino thì tiện hơn
docker exec -it bigdata-trino trino --execute "SELECT COUNT(*) FROM iceberg.silver.enriched_transactions"
docker exec -it bigdata-trino trino --execute "SELECT * FROM iceberg.silver.\"enriched_transactions\$snapshots\""
```

Hoặc xem trực quan ở MinIO Console http://localhost:9001.

---

## 7. Vấn đề thường gặp

| Triệu chứng | Nguyên nhân | Xử lý |
|---|---|---|
| `Path does not exist: s3a://data-lake-bronze/...` | Bronze chưa có dữ liệu, hoặc chưa tạo bucket | Chờ S3 sink flush; kiểm tra bucket |
| `NoSuchBucket` | Bucket lake chưa được tạo | [`run-all.md`](run-all.md) §3.1 |
| `ClassNotFoundException: S3AFileSystem` | Thiếu `--packages hadoop-aws` | Thêm đúng cặp package |
| `ClassNotFoundException` Iceberg | Thiếu `iceberg-spark-runtime` | Dùng `--packages` của §3.3 |
| OOM / treo khi join | Worker chỉ 2 GB; hoặc join raw CDC chưa dedup | Tăng RAM Docker; giảm `DURATION_SEC` khi sinh dữ liệu |
| Silver ít dòng bất thường | Inner join loại giao dịch có account không thấy trong Bronze | So `transactions.count()` với `enriched_count` trong log job |
| Partition `__HIVE_DEFAULT_PARTITION__` | `posted_at` NULL (giao dịch `pending` chưa post) | Bình thường; lọc `status` nếu không muốn |
| Iceberg treo lúc ghi | Dùng `S3FileIO` thay `HadoopFileIO` | Job đã ép `HadoopFileIO` — đừng đổi |
