# CDC & Kafka Connect — Debezium source + 6 sink

> Đăng ký, kiểm tra, sửa và xoá connector; publication + replication slot ở Postgres.
> Nguồn: [`debezium/`](../../debezium/), [`kafka-connect/`](../../kafka-connect/).
> Thiết kế: [ADR-0002](../decisions/0002-cdc-via-debezium-pgoutput.md),
> [ADR-0011](../decisions/0011-es-sink-upsert-by-primary-key.md).
> Cập nhật lần cuối: 2026-07-15.

---

## 1. Bảy connector

Tất cả chạy trên **một** worker Kafka Connect (`bigdata-kafka-connect`, REST ở `:8083`).

| Connector | File | Nguồn → Đích |
|---|---|---|
| `postgres-source-connector` | [`debezium/postgres-connector.json`](../../debezium/postgres-connector.json) | Postgres WAL → 4 topic `bankdb.public.*` |
| `es-sink-customers` | [`kafka-connect/es-sinks/es-sink-customers.json`](../../kafka-connect/es-sinks/es-sink-customers.json) | topic → ES index |
| `es-sink-accounts` | `es-sink-accounts.json` | topic → ES index |
| `es-sink-transactions` | `es-sink-transactions.json` | topic → ES index |
| `es-sink-transfers` | `es-sink-transfers.json` | topic → ES index |
| `es-sink-fraud-alerts` | `es-sink-fraud-alerts.json` | `fraud-alerts` → ES index |
| `s3-sink-cdc` | [`kafka-connect/s3-sinks/s3-sink-cdc.json`](../../kafka-connect/s3-sinks/s3-sink-cdc.json) | 4 topic → Parquet Bronze |

**Thứ tự đăng ký quan trọng:** Debezium **trước** — nó tạo topic và đăng ký Avro schema. Sink đăng ký
trước sẽ ở trạng thái chờ topic (`AUTO_CREATE_TOPICS_ENABLE=true` nên topic rỗng vẫn được tạo, khiến
lỗi khó thấy hơn).

---

## 2. Đăng ký

Config dùng `${env:...}` để lấy secret từ biến môi trường của worker (nhờ `EnvVarConfigProvider`) —
**không** có mật khẩu nào nằm trong file JSON.

```bash
# Source
curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json" \
  --data-binary "@debezium/postgres-connector.json"

# 5 ES sink
for f in customers accounts transactions transfers fraud-alerts; do
  curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json" \
    --data-binary "@kafka-connect/es-sinks/es-sink-$f.json"
done

# S3 sink
curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json" \
  --data-binary "@kafka-connect/s3-sinks/s3-sink-cdc.json"
```

---

## 3. Kiểm tra

```bash
curl.exe http://localhost:8083/connectors                          # liệt kê
curl.exe http://localhost:8083/connectors?expand=status            # tất cả + trạng thái
curl.exe http://localhost:8083/connectors/s3-sink-cdc/status       # một cái
curl.exe http://localhost:8083/connectors/s3-sink-cdc/config       # config đang chạy
```

Kỳ vọng `"state":"RUNNING"` cho cả connector lẫn mọi task. Task `FAILED` sẽ kèm stack trace trong
`trace` — đọc dòng đầu, thường đủ để biết nguyên nhân.

Xem bằng giao diện: **Kafka UI** http://localhost:8080 → tab **Kafka Connect**.

---

## 4. Sửa & xoá

```bash
# Cập nhật config (idempotent — dùng PUT, không cần xoá trước)
curl.exe -X PUT http://localhost:8083/connectors/s3-sink-cdc/config \
  -H "Content-Type: application/json" --data-binary "@config-moi.json"

# Restart connector / một task
curl.exe -X POST http://localhost:8083/connectors/s3-sink-cdc/restart
curl.exe -X POST http://localhost:8083/connectors/s3-sink-cdc/tasks/0/restart

# Tạm dừng / tiếp tục
curl.exe -X PUT http://localhost:8083/connectors/s3-sink-cdc/pause
curl.exe -X PUT http://localhost:8083/connectors/s3-sink-cdc/resume

# Xoá
curl.exe -X DELETE http://localhost:8083/connectors/s3-sink-cdc
```

> `PUT .../config` nhận **nội dung của khối `config`**, không phải cả file (file có bọc thêm `name` +
> `config`). `POST /connectors` mới nhận cả file.

---

## 5. Publication & replication slot (Postgres)

Debezium đọc WAL qua plugin `pgoutput` (có sẵn trong Postgres, không cần cài extension). Hai thứ nó
phụ thuộc:

| Thứ | Tên | Tạo bởi |
|---|---|---|
| Publication | `dbz_publication` | [`postgres/init/04_publication.sql`](../../postgres/init/04_publication.sql) — **tường minh 4 bảng**, không `FOR ALL TABLES` |
| Replication slot | `debezium_slot` | Debezium tự tạo lúc kết nối lần đầu |

`publication.autocreate.mode = disabled` → Debezium **không** tự tạo publication. Bảng nào thiếu trong
publication sẽ **im lặng không có CDC**, dù có mặt trong `table.include.list`.

```sql
-- Publication đang publish bảng nào?
SELECT pubname, schemaname, tablename FROM pg_publication_tables WHERE pubname = 'dbz_publication';

-- Slot còn sống? Đang giữ bao nhiêu WAL?
SELECT slot_name, active, restart_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS wal_giu_lai
FROM pg_replication_slots;
```

> ⚠️ **Slot mồ côi làm đầy đĩa.** Xoá connector **không** xoá slot. Slot không có consumer sẽ giữ WAL
> vô hạn cho tới khi hết đĩa. Nếu đã bỏ hẳn CDC:
> ```sql
> SELECT pg_drop_replication_slot('debezium_slot');
> ```

**Thêm bảng vào CDC** — phải sửa **hai** nơi cho khớp (chính là metadata sprawl #2 và #3 ở
[`../architecture/BDP-current-state.md`](../architecture/BDP-current-state.md) §3):

```sql
ALTER PUBLICATION dbz_publication ADD TABLE public.bang_moi;
```
```json
"table.include.list": "public.customers,...,public.bang_moi"
```

---

## 6. Các cấu hình đáng chú ý

### 6.1 Debezium source

| Cấu hình | Giá trị | Vì sao quan trọng |
|---|---|---|
| `topic.prefix` | `bankdb` | Quyết định tên topic: `bankdb.public.<table>`. Đổi = mọi consumer hạ nguồn phải đổi. |
| `snapshot.mode` | `initial` | Lần đầu snapshot toàn bộ 4 bảng với `op = 'r'`. Consumer lọc `op = 'c'` sẽ **bỏ qua** dữ liệu snapshot. |
| `decimal.handling.mode` | `string` | Mọi `NUMERIC` → STRING. Lý do Flink phải CAST và Spark phải cast double. [ADR-0003](../decisions/0003-avro-with-schema-registry.md) |
| `tombstones.on.delete` | `false` | DELETE không sinh message `null`. |
| `heartbeat.interval.ms` | `10000` | Giữ `restart_lsn` tiến lên cả khi bảng im lặng → tránh WAL phình. |
| `tasks.max` | `1` | Debezium Postgres **luôn** 1 task (một slot đọc tuần tự). Tăng lên vô nghĩa. |

### 6.2 ES sink

| Cấu hình | Giá trị | Ý nghĩa |
|---|---|---|
| `transforms` | `unwrap,extractKey` | `unwrap` bỏ envelope giữ `after`; `extractKey` lấy PK làm `_id` |
| `transforms.extractKey.field` | `transaction_id` (tuỳ bảng) | **Sai field = upsert hỏng**: mỗi event thành document mới |
| `key.ignore` | `false` | Dùng key của message làm `_id` → upsert đúng |
| `schema.ignore` | `true` | ES **tự suy mapping**, không lấy từ Avro. Xem cảnh báo dưới |
| `write.method` | `upsert` | Cùng PK → cập nhật, không nhân bản |
| `behavior.on.null.values` | `delete` | Tombstone → xoá document |

> **Hệ quả của `schema.ignore=true`:** ES đoán kiểu từ document đầu tiên. Vì `decimal.handling.mode=string`,
> `balance`/`amount` đến dưới dạng chuỗi → ES map thành `text`/`keyword`, và filter dạng số như
> `balance > 10000` **không** hoạt động. Filter theo `status` thay thế, hoặc khai báo index template
> trước khi sink chạy.

### 6.3 S3 sink

Xem [`../architecture/BDP-lakehouse-medallion.md`](../architecture/BDP-lakehouse-medallion.md) §2 cho
bảng đầy đủ. Ba điểm dễ vấp:

- `flush.size=1000` **hoặc** `rotate.interval.ms=300000` — cái nào tới trước. Tải thấp → file nhỏ, nhiều.
- `timestamp.extractor=Record` — partition theo timestamp **trong message**, nên replay cho ra cùng layout.
- Bucket `data-lake-bronze` **phải tồn tại trước**; connector không tự tạo.

---

## 7. DLQ — chưa được nối

**Không connector nào** trong repo có `errors.deadletterqueue.topic.name`. Vì vậy 6 topic `dlq.*` mà
`dlq-processor` subscribe **không bao giờ có message**. Xem
[`dlq-and-notifier.md`](dlq-and-notifier.md) và [ADR-0012](../decisions/0012-dlq-processor-not-wired.md).

Muốn bật, thêm vào từng sink config:

```json
"errors.tolerance": "all",
"errors.deadletterqueue.topic.name": "dlq.es-sink-transactions",
"errors.deadletterqueue.topic.replication.factor": "1",
"errors.deadletterqueue.context.headers.enable": "true"
```

`context.headers.enable` là **bắt buộc** — `dlq_processor.py` đọc các header `__connect.errors.*` để
phân loại lỗi. Thiếu nó thì mọi lỗi rơi vào nhóm `UNKNOWN`.
