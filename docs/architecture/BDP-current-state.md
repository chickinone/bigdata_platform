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
| 6 | ✅ ~~Schema Kafka source `ROW<...>`~~ | **ĐÃ SINH** cho cả metric runner lẫn fraud runner (từ cột contract, [ADR-0023](../decisions/0023-flink-metric-runner-declarative.md)); `lane1_dashboard.py` + `lane3_fraud_detection.py` đã xoá | ~~Sửa cột phải sửa nhiều chỗ~~ — ROW không còn viết tay ở đâu |
| 7 | `group.id` mỗi job | mỗi file Flink | Trùng `group.id` → tranh offset |
| 8 | ✅ ~~Schema metric đầu ra (Flink sink)~~ | **ĐÃ SINH** — `flink_sql.py` sinh sink DDL từ **cùng cột contract** với DDL ClickHouse ([ADR-0023](../decisions/0023-flink-metric-runner-declarative.md)); `lane1_dashboard.py` đã xoá | ~~Lệch thủ công với ClickHouse~~ — nay cùng nguồn, không thể lệch |
| 9 | ~~Schema metric ClickHouse~~ | **ĐÃ SINH** — [ADR-0019](../decisions/0019-generate-clickhouse-metric-ddl.md). 3 đối tượng/metric đọc chung một `columns` → không thể lệch | ~~MV im lặng bỏ dữ liệu~~ |
| 10 | 🟡 Cột join + output Silver | Nay khai trong batch spec `metadata/pipelines/batch/silver_enriched_transactions.yaml` (inputs + SQL + output columns), chạy bằng `medallion_runner` ([ADR-0024](../decisions/0024-spark-medallion-runner-sql.md)); `enrich_transactions.py` đã xoá. Gold (#11) còn lại | Silver: phụ thuộc + schema khai tường minh |
| 11 | ✅ ~~Cột tổng hợp Gold~~ | **ĐÃ KHAI** trong 3 batch spec `metadata/pipelines/batch/gold_*.yaml` (SQL + output columns), chạy bằng `medallion_runner` ([ADR-0024](../decisions/0024-spark-medallion-runner-sql.md)); parity 339/100/310 = job cũ → `build_gold_layer.py` đã xoá | ~~Không ai biết Gold phụ thuộc cột nào~~ — nay khai tường minh |
| 12 | Danh sách DLQ topic | [`dlq-processor/dlq_processor.py`](../../dlq-processor/dlq_processor.py) | Thêm connector phải nhớ thêm DLQ |
| 13 | Catalog nguồn | [`trino/etc/catalog/`](../../trino/etc/catalog/) | Cấu hình rời rạc |

### 3.1 Minh chứng — cùng khối `ROW<...>` copy-paste trong các file Flink

Lúc audit, khối này bị lặp trong **6 file**. Xoá 4 file di sản print-sink (2026-07-16), rồi
**`lane1_dashboard.py`** (2026-07-18, thay bằng metric runner sinh — [ADR-0023](../decisions/0023-flink-metric-runner-declarative.md)).
Nay **KHÔNG còn file nào** viết tay khối này — sprawl #6 **hết hẳn**:

```text
metric_runner.py : ROW sinh từ cột tham chiếu của pipeline metric   (đã sinh)
fraud_runner.py  : ROW sinh từ source_columns của fraud.yaml         (đã sinh)
```

*(Đã xoá: `lane1_{timeseries,kpi,breakdown,topn}.py`, `lane1_dashboard.py`, `lane3_fraud_detection.py`.)*

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

Lúc audit: 4 metric × 3 = **12 khối schema viết tay phải khớp nhau tuyệt đối**, cộng
`CREATE TABLE ... _sink` trong Flink = 16 khối cho 4 metric.

> **Đã xử lý 12/16** ([ADR-0019](../decisions/0019-generate-clickhouse-metric-ddl.md)): cả 3 đối tượng
> ClickHouse nay sinh từ **một** `columns` của contract → không thể lệch. **Còn hở 4 khối** — sink DDL
> bên Flink vẫn viết tay, nên vẫn có thể lệch với ClickHouse. Hết hẳn ở **Pha 3**.

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
| 2 | ✅ ~~**Bucket lake không được tạo**~~ | **ĐÃ XỬ LÝ** 2026-07-18 — `minio-init` nay tạo cả `data-lake-{bronze,silver,gold,iceberg}` | S3 sink chạy được ngay khi `up`; không phải tạo tay. |
| 3 | ~~**DLQ chưa được nối**~~ | **ĐÃ XỬ LÝ** 2026-07-16 — [ADR-0017](../decisions/0017-dlq-flow-observe-then-park.md) | Mọi sink bật DLQ; lỗi chảy vào `metrics.dlq_events`. Kèm sửa lỗi replay làm hỏng metric. |
| 4 | ~~**`metrics.dlq_events` không tồn tại**~~ | **ĐÃ XỬ LÝ** — [`clickhouse/init/03_dlq.sql`](../../clickhouse/init/03_dlq.sql) | Vẫn phải chạy tay như mọi init ClickHouse (mục 1). |
| 4b | **`metrics.notification_events` không tồn tại** | `fraud_notifier.py:177` INSERT vào bảng này; không init nào tạo nó | Mọi lần ghi đều fail (nuốt trong `try/except`). **Còn nợ.** |
| 5 | ~~**4 job Flink trùng lặp**~~ | **ĐÃ XÓA** 2026-07-16 — `lane1_{timeseries,kpi,breakdown,topn}.py` (di sản print-sink) | Còn 2 file Flink; hết bẫy chạy trùng topic. |
| 6 | ✅ ~~**Print sink còn trong job fraud**~~ | **ĐÃ BỎ** — `fraud_runner.py` (thay `lane3_fraud_detection.py`) không còn `ds.print` ([ADR-0023](../decisions/0023-flink-metric-runner-declarative.md)) | ~~In mọi transaction ra log ở 150 RPS~~ |

### 4.2 Khoảng trống production

| # | Vấn đề | Chi tiết |
|---|---|---|
| 7 | **Single-node mọi thứ** | Kafka RF=1, ES single-node, 1 Spark worker, checkpoint 30s. Không HA, không chịu lỗi. |
| 8 | ✅ ~~**`AUTO_CREATE_TOPICS_ENABLE=true`**~~ | **ĐÃ TẮT** — topic tạo tường minh qua bản kê sinh từ registry + service `kafka-init`; `auto.create.topics=false`, chứng minh live 21/21 topic khớp và topic ma không bị tạo ([ADR-0020](../decisions/0020-generate-kafka-topic-manifest.md)). |
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
