# Tình trạng hiện tại — đánh giá & audit metadata sprawl

> Tài liệu này mô tả hệ thống **như nó đang tồn tại trong repo**, không phải mục tiêu. Nó lý giải *vì
> sao* cần metadata-driven. Mục tiêu và cách chuyển đổi nằm ở
> [`../roadmap/BDP-metadata-driven-roadmap.md`](../roadmap/BDP-metadata-driven-roadmap.md).
> Cập nhật lần cuối: 2026-07-15.

---

## 1. Đánh giá tổng quát

Hệ thống **chạy được end-to-end** và bao phủ rất nhiều pattern kỹ thuật thật: CDC qua WAL, exactly-once
checkpoint, medallion lakehouse, Iceberg time-travel, phát hiện gian lận có state, DLQ, federation.
Về mặt chức năng, đây là một nền tảng **tốt**.

Rào cản để lên production **không phải là thiếu công cụ**. Nó là **cách khai báo và vận hành cấu
hình**: cùng một "sự thật về dữ liệu" đang bị sao chép thủ công ở khoảng 10 nơi. Nói ngắn gọn — nền
tảng *chức năng tốt* nhưng *vận hành thủ công*.

---

## 2. Ma trận độ trưởng thành

| Khía cạnh | Hiện tại | Mục tiêu production |
|---|---|---|
| Nguồn sự thật schema | ❌ Rải rác ~10 nơi | ✅ 1 registry / data contract |
| Thêm cột/bảng | ❌ Sửa tay nhiều file | ✅ Sửa 1 contract → sinh tự động |
| Triển khai artifact | ❌ Thủ công qua REST/`docker compose` | ✅ Generator + CI/CD |
| Kiểm soát schema evolution | ❌ Không (Schema Registry có nhưng không gate) | ✅ Compatibility gate |
| Lineage / catalog | ❌ Không | ✅ Tự động (DataHub/OpenMetadata) |
| Data quality | ❌ Không | ✅ Rule trong metadata + gate |
| Orchestration | ❌ Chạy tay, không lịch, không retry | ✅ DAG sinh từ phụ thuộc dataset |
| Quản lý secrets | ⚠️ `.env` local, đã gitignore, không có manager | ✅ Vault/SOPS + `secret_ref` |
| HA / chịu lỗi | ❌ Single node toàn bộ | ⚠️ Ngoài phạm vi metadata (nhưng cần) |

---

## 3. VẤN ĐỀ CỐT LÕI — "Metadata Sprawl"

Đây là phần quan trọng nhất của tài liệu.

Một "sự thật về schema" — thực thể có những cột nào, khóa gì, đi vào topic nào — **không** được lưu ở
một chỗ. Nó bị **sao chép thủ công** khắp hệ thống. Bảng dưới liệt kê, chỉ với **một** thực thể
`transactions`, tất cả những nơi cùng thông tin đó đang được khai báo lại bằng tay:

| # | Sự thật về `transactions` | Nơi hardcode | Rủi ro khi lệch |
|---|---|---|---|
| 1 | Bảng + danh sách cột | [`postgres/init/02_schema.sql`](../../postgres/init/02_schema.sql) | Nguồn sự thật thật sự |
| 2 | Bảng được publish cho CDC | [`postgres/init/04_publication.sql`](../../postgres/init/04_publication.sql) | Quên → không có CDC |
| 3 | Bảng nằm trong CDC | [`debezium/postgres-connector.json`](../../debezium/postgres-connector.json) (`table.include.list`) | Lệch với publication |
| 4 | Topic → Bronze S3 | [`kafka-connect/s3-sinks/s3-sink-cdc.json`](../../kafka-connect/s3-sinks/s3-sink-cdc.json) (`topics`) | Thiếu topic → mất dữ liệu lake |
| 5 | Topic → ES + khóa PK | [`kafka-connect/es-sinks/es-sink-transactions.json`](../../kafka-connect/es-sinks/es-sink-transactions.json) | Sai `extractKey.field` → upsert hỏng |
| 6 | Schema Kafka source `ROW<...>` | [`flink/jobs/`](../../flink/jobs/) — **lặp trong 6 file** | Thêm cột phải sửa 6 chỗ |
| 7 | `group.id` mỗi job | mỗi file Flink | Trùng `group.id` → tranh offset |
| 8 | Schema metric đầu ra | [`flink/jobs/lane1_dashboard.py`](../../flink/jobs/lane1_dashboard.py) | Phải khớp thủ công với ClickHouse |
| 9 | Schema metric ClickHouse | [`clickhouse/init/`](../../clickhouse/init/) — **lặp 3 lần/metric** (target + kafka + MV) | Lệch cột → MV **im lặng bỏ dữ liệu** |
| 10 | Cột join + output Silver | [`spark/jobs/enrich_transactions.py`](../../spark/jobs/enrich_transactions.py) | Đổi cột nguồn → job vỡ |
| 11 | Cột tổng hợp Gold | [`spark/jobs/build_gold_layer.py`](../../spark/jobs/build_gold_layer.py) | Không ai biết Gold phụ thuộc cột nào |
| 12 | Danh sách DLQ topic | [`dlq-processor/dlq_processor.py`](../../dlq-processor/dlq_processor.py) | Thêm connector phải nhớ thêm DLQ |
| 13 | Catalog nguồn | [`trino/etc/catalog/`](../../trino/etc/catalog/) | Cấu hình rời rạc |

### 3.1 Minh chứng — cùng khối `ROW<...>` copy-paste trong 6 file Flink

```text
lane1_breakdown.py     : 'topic' = 'bankdb.public.transactions'
lane1_dashboard.py     : 'topic' = 'bankdb.public.transactions'  (+ 4 sink topic)
lane1_kpi.py           : 'topic' = 'bankdb.public.transactions'
lane1_timeseries.py    : 'topic' = 'bankdb.public.transactions'
lane1_topn.py          : 'topic' = 'bankdb.public.transactions'
lane3_fraud_detection  : 'topic' = 'bankdb.public.transactions'
```

Mỗi file lặp lại y hệt:

```sql
ROW<transaction_id BIGINT, account_id BIGINT, transaction_type STRING,
    amount STRING, currency STRING, status STRING>
```

### 3.2 Minh chứng — schema metric ClickHouse lặp 3 lần mỗi metric

Với `timeseries`:
1. Bảng đích `metrics.timeseries` (ReplacingMergeTree)
2. Bảng đệm `metrics.timeseries_kafka` (Kafka engine)
3. `SELECT` trong `metrics.timeseries_mv` (Materialized View)

→ 4 metric × 3 = **12 khối schema viết tay phải khớp nhau tuyệt đối**, và còn phải khớp với
`CREATE TABLE ... _sink` trong Flink. Tổng cộng 16 khối cho 4 metric.

### 3.3 Hệ quả trực tiếp

- **Thêm 1 cột** vào `transactions`: sửa Postgres DDL → sửa `ROW<...>` ở tối đa 6 file Flink → nếu cột
  vào metric thì sửa ClickHouse (3 nơi) → sửa `SELECT` Spark → cân nhắc ES mapping. **Rất dễ bỏ sót.**
- **Thêm 1 bảng mới**: đụng tối thiểu 8 file ở 5 công cụ khác nhau.
- **Không có single source of truth**: không thể trả lời tự động "cột X đi tới đâu?", "ai sở hữu
  dataset này?", "PII nằm ở đâu?".
- **Lệch schema âm thầm**: MV ClickHouse lệch cột sẽ *bỏ qua dữ liệu không báo lỗi* — hỏng kiểu tệ
  nhất, vì dashboard vẫn xanh.
- **Không tự động hóa được**: mọi thay đổi là thao tác người, không review được bằng diff có nghĩa.

---

## 4. Khoảng trống đã biết (ngoài metadata)

Xác minh trực tiếp từ code, không phải suy đoán.

### 4.1 Khoảng trống chức năng — thứ *trông như* chạy nhưng không

| # | Vấn đề | Bằng chứng | Hệ quả |
|---|---|---|---|
| 1 | **ClickHouse init không tự chạy** | `docker-compose.yml` không mount `clickhouse/init/` vào `/docker-entrypoint-initdb.d` | Không bảng `metrics.*` → MV không tồn tại → Grafana rỗng. Phải chạy tay. |
| 2 | **Bucket lake không được tạo** | `minio-init` chỉ `mc mb` cho `flink-checkpoints` và `flink-savepoints` | S3 sink fail cho tới khi tạo tay `data-lake-{bronze,silver,gold,iceberg}`. |
| 3 | **DLQ chưa được nối** | Không file nào trong `kafka-connect/` hay `debezium/` có `errors.deadletterqueue.*` | 6 topic `dlq.*` không bao giờ có message → `dlq-processor` chạy vô ích. |
| 4 | **`metrics.dlq_events` không tồn tại** | `dlq_processor.py:80` INSERT vào bảng này; `clickhouse/init/01_schema.sql` không tạo nó | Kể cả có DLQ, mọi lần ghi đều fail (đã bắt lỗi và chỉ log). |
| 5 | **4 job Flink trùng lặp** | `lane1_{timeseries,kpi,breakdown,topn}.py` đã được gộp vào `lane1_dashboard.py` | Chạy song song → ghi trùng vào cùng topic metric. |
| 6 | **Print sink còn trong job fraud** | `lane3_fraud_detection.py` giữ `ds.print("LANE3-RAW")` | In **mọi** transaction ra log TaskManager ở 150 RPS. |

### 4.2 Khoảng trống production

| # | Vấn đề | Chi tiết |
|---|---|---|
| 7 | **Single-node mọi thứ** | Kafka RF=1, ES single-node, 1 Spark worker, checkpoint 30s. Không HA, không chịu lỗi. |
| 8 | **`AUTO_CREATE_TOPICS_ENABLE=true`** | Topic tự sinh với partition/retention mặc định, không kiểm soát. |
| 9 | **Không orchestration** | Job Spark chạy tay; không lịch, không phụ thuộc, không retry/backfill. |
| 10 | **Không data quality gate & lineage** | Không kiểm tra chất lượng, không truy vết nguồn-đích tự động. |
| 11 | **Không CI/CD** | Thay đổi cấu hình áp thủ công qua REST/`docker compose`. |
| 12 | **Không auth ở hầu hết service** | ES tắt security (`xpack.security.enabled=false`), Kafka PLAINTEXT, Trino/MinIO/Kafka UI không auth. |
| 13 | **Silver là full refresh** | `enrich_transactions.py` dùng `mode("overwrite")` — đọc lại toàn bộ Bronze mỗi lần chạy. |

### 4.3 Secrets — đánh giá chính xác

> **Đính chính một hiểu nhầm phổ biến.** Có tài liệu nội bộ từng mô tả đây là "sự cố P0: `.env` bị
> commit vào Git". **Điều đó không đúng.** Kiểm chứng:
> ```bash
> git ls-files | grep env        # → không kết quả
> git log --all -- .env          # → không có commit nào
> cat .gitignore                 # → dòng đầu tiên là `.env`
> ```
> `.env` **chưa từng** được commit và **đã** được gitignore từ đầu.

Rủi ro thật, hẹp hơn nhưng vẫn đáng xử lý:

- Secrets thật (mật khẩu Postgres/ClickHouse, App Password Gmail) nằm **dạng plaintext trong file
  `.env` trên máy local**, không có secret manager và không có lịch xoay vòng.
- Chưa có bước quét secret trong CI để chặn commit nhầm về sau — hiện chỉ có `.gitignore` bảo vệ.
- Compose truyền secret qua biến môi trường, nên chúng lộ ra trong `docker inspect` và log container.

Đánh giá: đây là **giới hạn đã biết của môi trường lab**, không phải rò rỉ. Đúng mức ưu tiên là
"xử lý trước khi lên production", chứ không phải "thu hồi khẩn cấp".
Xem [ADR-0013](../decisions/0013-secrets-in-gitignored-env.md).

---

## 5. Kết luận

Hệ thống **đã chứng minh được kiến trúc** và chạy đúng chức năng. Rào cản để lên production không phải
"thiếu công cụ" mà là **cách khai báo và vận hành cấu hình**: mọi thứ đang bị hardcode và trùng lặp.

Chuyển sang **metadata-driven** chính là gỡ bỏ sự trùng lặp đó bằng cách tập trung "sự thật về dữ
liệu" vào một registry và sinh mọi artifact từ đó. Lộ trình chi tiết:
[`../roadmap/BDP-metadata-driven-roadmap.md`](../roadmap/BDP-metadata-driven-roadmap.md).

Trước khi bắt đầu lộ trình đó, các mục **4.1** (khoảng trống chức năng) nên được vá — chúng rẻ, và
việc tự động hoá trên một nền còn hỏng chỉ khiến chỗ hỏng lan nhanh hơn.
