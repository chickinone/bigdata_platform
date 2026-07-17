# Mô hình dữ liệu — nguồn, metric, lakehouse

> Mọi schema trong hệ thống, theo thứ tự dữ liệu chảy qua: 4 bảng OLTP → topic CDC → 4 bảng metric
> ClickHouse → các tầng lake. Phải khớp với
> [`postgres/init/02_schema.sql`](../../postgres/init/02_schema.sql) và
> [`clickhouse/init/01_schema.sql`](../../clickhouse/init/01_schema.sql).
> Cập nhật lần cuối: 2026-07-15.

---

## 1. Bốn thực thể nguồn (PostgreSQL)

Nghiệp vụ mô phỏng: khách hàng mở tài khoản, phát sinh giao dịch, và chuyển tiền cho nhau.

| Thực thể | Loại | Khóa chính | Replica identity | Vì sao |
|---|---|---|---|---|
| `customers` | Dimension chậm đổi | `customer_id` | DEFAULT | Chỉ cần PK để định danh; `updated_at` dùng để dedup ở Silver. |
| `accounts` | Semi-dimension | `account_id` | **FULL** | `balance` đổi liên tục → cần before-image để audit thay đổi số dư. |
| `transactions` | Fact append-only | `transaction_id` | DEFAULT | Chỉ INSERT, không UPDATE → before-image vô nghĩa. Throughput cao nhất. |
| `transfers` | Fact có lifecycle | `transfer_id` | **FULL** | Có state machine `pending → processing → completed/failed/cancelled` → cần biết trạng thái cũ. |

Chi tiết đánh đổi của `REPLICA IDENTITY FULL`:
[ADR-0004](../decisions/0004-replica-identity-full-for-mutable-tables.md).

### 1.1 Cột theo bảng

**`customers`**

| Cột | Kiểu | Ghi chú |
|---|---|---|
| `customer_id` | BIGSERIAL PK | |
| `full_name` | VARCHAR(200) NOT NULL | **PII** |
| `email` | VARCHAR(200) UNIQUE NOT NULL | **PII** |
| `phone` | VARCHAR(20) | **PII** |
| `country_code` | CHAR(2) NOT NULL | |
| `kyc_status` | VARCHAR(20) | CHECK: `pending`/`verified`/`rejected`/`expired` |
| `risk_score` | SMALLINT | CHECK: 0–100, mặc định 50 |
| `created_at`, `updated_at` | TIMESTAMPTZ | `updated_at` là khóa dedup ở Silver |

**`accounts`**

| Cột | Kiểu | Ghi chú |
|---|---|---|
| `account_id` | BIGSERIAL PK | |
| `customer_id` | BIGINT FK → `customers` | |
| `account_number` | VARCHAR(20) UNIQUE | **PII** |
| `account_type` | VARCHAR(20) | CHECK: `checking`/`savings`/`credit`/`investment` |
| `currency` | CHAR(3) | mặc định `USD` |
| `balance` | NUMERIC(19,4) | **đổi liên tục** → lý do dùng REPLICA IDENTITY FULL |
| `status` | VARCHAR(20) | CHECK: `active`/`frozen`/`closed`/`suspended` |
| `opened_at`, `closed_at`, `updated_at` | TIMESTAMPTZ | |

**`transactions`** — bảng nóng nhất, là nguồn của cả Lane 1 lẫn Lane 3.

| Cột | Kiểu | Ghi chú |
|---|---|---|
| `transaction_id` | BIGSERIAL PK | khóa upsert của ES sink |
| `account_id` | BIGINT FK → `accounts` | khóa `key_by` của fraud detector |
| `transaction_type` | VARCHAR(20) | CHECK: `deposit`/`withdrawal`/`fee`/`interest`/`transfer_in`/`transfer_out` |
| `amount` | NUMERIC(19,4) | CHECK > 0. **Sang Avro dưới dạng STRING** — xem §2.2 |
| `balance_after` | NUMERIC(19,4) | |
| `currency` | CHAR(3) | |
| `merchant_name`, `merchant_category`, `description` | VARCHAR/TEXT | |
| `status` | VARCHAR(20) | CHECK: `pending`/`completed`/`failed`/`reversed`. `failed` kích hoạt Failed Storm detector |
| `created_at`, `posted_at` | TIMESTAMPTZ | `posted_at` là khóa partition năm/tháng/ngày ở Silver |

**`transfers`**

| Cột | Kiểu | Ghi chú |
|---|---|---|
| `transfer_id` | BIGSERIAL PK | |
| `from_account_id`, `to_account_id` | BIGINT FK | CHECK: hai bên phải khác nhau |
| `amount` | NUMERIC(19,4) | CHECK > 0 |
| `currency` | CHAR(3) | |
| `status` | VARCHAR(20) | state machine 5 trạng thái |
| `reference_code` | VARCHAR(40) UNIQUE | |
| `failure_reason` | TEXT | |
| `initiated_at`, `completed_at`, `updated_at` | TIMESTAMPTZ | |

---

## 2. Tầng CDC (Kafka)

### 2.1 Ánh xạ bảng → topic

Debezium dùng `topic.prefix = bankdb`, nên tên topic là `<prefix>.<schema>.<table>`:

```text
public.customers     → bankdb.public.customers
public.accounts      → bankdb.public.accounts
public.transactions  → bankdb.public.transactions
public.transfers     → bankdb.public.transfers
```

Chỉ 4 bảng này được publish, khai báo **hai lần** (đây chính là metadata sprawl — xem
[`BDP-current-state.md`](BDP-current-state.md) §3):
- [`postgres/init/04_publication.sql`](../../postgres/init/04_publication.sql) — `CREATE PUBLICATION dbz_publication FOR TABLE ...`
- [`debezium/postgres-connector.json`](../../debezium/postgres-connector.json) — `table.include.list`

Nếu hai danh sách lệch nhau, bảng thiếu sẽ **im lặng không có CDC**.

### 2.2 Hình dạng message

Mỗi message là một Debezium envelope Avro:

```text
{ op: 'c'|'u'|'d'|'r', ts_ms: <long>, before: {...}|null, after: {...}|null, source: {...} }
```

Ba quy ước ảnh hưởng tới mọi consumer hạ nguồn:

| Cấu hình | Giá trị | Hệ quả |
|---|---|---|
| `decimal.handling.mode` | `string` | Mọi cột `NUMERIC` **sang Avro thành STRING**. Vì vậy Flink phải `CAST(after.amount AS DECIMAL(19,4))` và Spark phải `.cast("double")`. Xem [ADR-0003](../decisions/0003-avro-with-schema-registry.md). |
| `snapshot.mode` | `initial` | Lần đầu chạy sẽ snapshot toàn bộ 4 bảng (`op = 'r'`), sau đó mới stream. Consumer lọc `op = 'c'` sẽ **bỏ qua** dữ liệu snapshot. |
| `tombstones.on.delete` | `false` | DELETE không sinh message tombstone `null`. |

> **Bẫy thường gặp:** cả Lane 1 và Lane 3 đều lọc `WHERE op = 'c'` — chỉ tính bản ghi INSERT mới.
> Hàng snapshot (`op = 'r'`) không vào metric.

---

## 3. Tầng metric (ClickHouse)

4 bảng, tất cả `ReplacingMergeTree(inserted_at)` + partition theo ngày + TTL. Flink ghi ra Kafka
`metrics.<tên>`, ClickHouse kéo về bằng Kafka engine + MV — chi tiết ở
[ADR-0007](../decisions/0007-clickhouse-kafka-engine-serving.md).

| Bảng | Window | Khoá sắp xếp | TTL | Nội dung |
|---|---|---|---|---|
| `metrics.timeseries` | TUMBLE 1 phút | `(window_start, tx_type)` | 30 ngày | `tx_count`, `total_amount` theo loại giao dịch. |
| `metrics.kpi` | CUMULATE 5 phút / 1 ngày | `(window_start, window_end)` | **90 ngày** | 6 KPI: tổng số, tổng giá trị, thành công, thất bại, tỉ lệ thành công, số user hoạt động. |
| `metrics.breakdown` | CUMULATE 5 phút / 1 ngày | `(window_start, window_end, tx_type)` | 30 ngày | Như KPI nhưng chẻ theo `tx_type`. |
| `metrics.topn` | CUMULATE 5 phút / 1 ngày | `(window_start, window_end, rank_num)` | 30 ngày | Top 10 account theo `total_value`. |

**Mỗi metric cần 3 đối tượng** phải khớp tuyệt đối với nhau và với sink DDL trong Flink:

1. Bảng đích `metrics.<m>` — `ReplacingMergeTree` ([`01_schema.sql`](../../clickhouse/init/01_schema.sql))
2. Bảng đệm `metrics.<m>_kafka` — Kafka engine, `JSONEachRow` ([`02_kafka_consumers.sql`](../../clickhouse/init/02_kafka_consumers.sql))
3. `SELECT` trong `metrics.<m>_mv` — Materialized View

> ✅ **Cả 3 nay SINH từ một contract** ([ADR-0019](../decisions/0019-generate-clickhouse-metric-ddl.md)) —
> `metadata/datasets/metrics/*.yaml`. Chúng đọc chung một `columns` nên **không thể lệch**, hết cảnh MV
> bỏ dữ liệu âm thầm. Hai file `01_schema.sql`/`02_kafka_consumers.sql` là **file sinh, đừng sửa tay**.
>
> ⚠️ **Vẫn còn hở:** sink DDL bên Flink (`lane1_dashboard.py`) **viết tay** → vẫn có thể lệch với
> ClickHouse. Hết khi Flink runner sinh cả hai đầu từ cùng spec (Pha 3).

### 3.1 `metrics.dlq_events` — lỗi cũng là dữ liệu

Cùng database `metrics` nhưng **không phải metric**: đây là nơi lỗi của Kafka Connect trở thành dữ
liệu truy vấn được ([ADR-0017](../decisions/0017-dlq-flow-observe-then-park.md)). Vẫn theo đúng pattern
3 đối tượng như metric (`dlq_events` + `_kafka` + `_mv`), vì cùng lý do ở
[ADR-0007](../decisions/0007-clickhouse-kafka-engine-serving.md).

| Nhóm cột | Cột | Ý nghĩa |
|---|---|---|
| Định danh | `connector_name`, `dlq_topic`, `original_topic` | Lỗi ở đâu |
| Phân loại | `category` (`TRANSIENT`/`PERMANENT`/`UNKNOWN`), `action` (`PARKED`) | Loại lỗi + đã làm gì |
| Chẩn đoán | `error_class`, `error_stage`, `error_message` | Vì sao lỗi |
| **Vị trí trong DLQ** | `dlq_partition`, `dlq_offset` | **Khoá chống trùng** (khoá tự nhiên của message Kafka) |
| **Vị trí trong topic gốc** | `original_partition`, `original_offset` | **Để tìm lại bản ghi mà phát lại** |
| Khác | `message_key`, `message_size`, `detected_at` | |

Hai cặp vị trí **khác nhau, đừng trộn**. Bản đầu tiên chỉ lưu vị trí trong DLQ — tức là không có đường
về để tìm bản ghi đã lỗi. Lỗi đó chỉ lộ ra khi chạy thử thật.

Engine: `ReplacingMergeTree(inserted_at)` `ORDER BY (dlq_topic, dlq_partition, dlq_offset)`, TTL 30
ngày. Processor là at-least-once nên restart có thể đọc lại — dedup làm việc đó vô hại.

> **Nội dung message KHÔNG được lưu**, chỉ `message_key`. Nội dung gốc đã nằm nguyên trong topic DLQ;
> chép sang ClickHouse là nhân bản PII (`customers` có `full_name`/`email`/`phone`) ra thêm một chỗ,
> giữ 30 ngày, không thêm giá trị điều tra.

> `metrics.notification_events` (fraud-notifier ghi vào) thì **vẫn chưa tồn tại** — cùng loại lỗi,
> chưa xử lý. Xem [ADR-0012](../decisions/0012-dlq-processor-not-wired.md).

---

## 4. Tầng lakehouse (MinIO + Iceberg)

| Tầng | Đường dẫn | Định dạng | Ghi bởi | Nội dung |
|---|---|---|---|---|
| **Bronze** | `s3a://data-lake-bronze/topics/<topic>/year=/month=/day=/hour=` | Parquet + snappy | S3 sink connector | Raw CDC đã `unwrap` (chỉ phần `after`), partition theo giờ. |
| **Silver** | `s3a://data-lake-silver/enriched_transactions/` | Parquet, partition `year/month/day` | `enrich_transactions.py` | Transaction đã join account + customer, dedup về current state. |
| **Gold** | `s3a://data-lake-gold/<bảng>/` | Parquet | `build_gold_layer.py` | 3 bảng: `daily_transaction_summary`, `customer_lifetime_metrics`, `high_risk_transactions`. |
| **Iceberg** | `s3a://data-lake-iceberg/warehouse/` | Iceberg | `silver_to_iceberg.py` | `lakehouse.silver.enriched_transactions` — có snapshot + time travel. |

Chi tiết dedup/join/snapshot: [`BDP-lakehouse-medallion.md`](BDP-lakehouse-medallion.md).

> Các bucket `data-lake-*` **không** được `minio-init` tạo (nó chỉ tạo `flink-checkpoints` và
> `flink-savepoints`). Phải tạo tay trước khi chạy S3 sink — xem
> [`../guide/run-all.md`](../guide/run-all.md) §3.

---

## 5. Tầng search (Elasticsearch)

5 index, upsert theo PK ([ADR-0011](../decisions/0011-es-sink-upsert-by-primary-key.md)):

| Index | Nguồn topic | Khóa document (`extractKey.field`) |
|---|---|---|
| `bankdb.public.customers` | cùng tên | `customer_id` |
| `bankdb.public.accounts` | cùng tên | `account_id` |
| `bankdb.public.transactions` | cùng tên | `transaction_id` |
| `bankdb.public.transfers` | cùng tên | `transfer_id` |
| `fraud-alerts` | `fraud-alerts` | — (không extract key) |

`schema.ignore=true` → ES **tự suy mapping** thay vì lấy từ Avro schema. Vì vậy các cột như `balance`
có thể bị map thành text/keyword và filter dạng số (`balance > 10000`) sẽ không hoạt động như mong đợi.

---

## 6. Vòng đời một cột — ví dụ `transactions.amount`

Theo dấu một cột qua toàn hệ thống cho thấy vì sao thêm cột lại tốn công đến vậy:

| Chặng | `amount` xuất hiện thế nào |
|---|---|
| PostgreSQL | `NUMERIC(19,4)`, CHECK > 0 |
| Debezium/Avro | **STRING** (do `decimal.handling.mode=string`) |
| Flink source DDL | `amount STRING` trong khối `ROW<...>` — **lặp ở 6 file** |
| Flink aggregation | `SUM(CAST(after.amount AS DECIMAL(19,4)))` |
| Kafka `metrics.*` | `total_amount` / `total_value` dạng JSON number |
| ClickHouse | `Decimal(19,4)` — khai báo 3 lần cho mỗi metric |
| Bronze Parquet | STRING (giữ nguyên như Avro) |
| Spark Silver | STRING, chuyển sang double bằng `.cast("double")` ở Gold |
| Elasticsearch | tự suy mapping (`schema.ignore=true`) |

**Chín nơi cho một cột.** Đó là "metadata sprawl" trong một dòng —
[`BDP-current-state.md`](BDP-current-state.md) §3 đo đầy đủ.
