# DLQ processor & fraud-notifier — hai consumer phụ trợ

> Hai service Python nhỏ đọc Kafka và làm việc ngoài luồng dữ liệu chính. **Cả hai đều thiếu wiring** —
> tài liệu này nói rõ thiếu gì và cần gì để chúng chạy thật.
> Nguồn: [`dlq-processor/`](../../dlq-processor/), [`fraud-notifier/`](../../fraud-notifier/).
> Xem [ADR-0012](../decisions/0012-dlq-processor-not-wired.md).
> Cập nhật lần cuối: 2026-07-15.

---

## 1. Tóm tắt tình trạng

| Service | Code | Trạng thái thật |
|---|---|---|
| `dlq-processor` | Đầy đủ, hợp lý | ❌ **Không bao giờ nhận message** — không connector nào bật DLQ. Và bảng đích không tồn tại. |
| `fraud-notifier` | Đầy đủ, hợp lý | ⚠️ **Email chạy được**; nhưng ghi ClickHouse luôn fail — bảng đích không tồn tại. |

**Cả hai bảng ClickHouse mà chúng ghi vào đều không có trong init schema:**

```bash
$ grep -c "dlq_events\|notification_events" clickhouse/init/01_schema.sql
0
```

[`clickhouse/init/01_schema.sql`](../../clickhouse/init/01_schema.sql) chỉ tạo `timeseries`, `kpi`,
`breakdown`, `topn`. Không có `metrics.dlq_events`, không có `metrics.notification_events`.

Cả hai service đều bọc lệnh ghi trong `try/except` và chỉ `log.error(...)`, nên chúng **không crash** —
chúng thất bại âm thầm. Đó là lý do vấn đề này tồn tại lâu mà không lộ.

---

## 2. DLQ processor

### 2.1 Ý định thiết kế

[`dlq_processor.py`](../../dlq-processor/dlq_processor.py) subscribe 6 topic DLQ, đọc header
`__connect.errors.*` của Kafka Connect, phân loại lỗi rồi hành động:

| Nhóm | Ví dụ exception | Hành động |
|---|---|---|
| `TRANSIENT` | `RetriableException`, `ConnectException`, `SocketTimeoutException`, ES `ResponseException` | **Tự động replay** về topic gốc |
| `PERMANENT` | `DataException`, `SerializationException`, `SchemaException` | Log cảnh báo, cần người xem |
| `UNKNOWN` | mọi thứ khác | Log cảnh báo |

Đây là một thiết kế tốt: lỗi hạ tầng tạm thời thì thử lại, lỗi dữ liệu hỏng thì không thử lại vô ích.

### 2.2 Vì sao nó không chạy

**Vấn đề 1 — không ai sinh message DLQ.** Nó subscribe:
```text
dlq.es-sink-customers, dlq.es-sink-accounts, dlq.es-sink-transactions,
dlq.es-sink-transfers, dlq.es-sink-fraud-alerts, dlq.s3-sink-cdc
```
Nhưng không file nào trong [`kafka-connect/`](../../kafka-connect/) có `errors.deadletterqueue.*`:
```bash
$ grep -rl "deadletterqueue" kafka-connect/ debezium/
# không kết quả
```
Vì `AUTO_CREATE_TOPICS_ENABLE=true`, 6 topic này **được tạo rỗng** khi consumer subscribe — nên
service báo "Connected to Kafka, monitoring 6 topics" và trông như đang khoẻ. Nó chỉ đang chờ mãi mãi.

**Vấn đề 2 — bảng đích không tồn tại.** `dlq_processor.py:80` INSERT vào `metrics.dlq_events`, bảng
này chưa từng được tạo.

### 2.3 Cần gì để nó chạy thật

**Bước 1 — tạo bảng:**
```sql
CREATE TABLE IF NOT EXISTS metrics.dlq_events (
    dlq_topic       LowCardinality(String),
    original_topic  LowCardinality(String),
    connector_name  LowCardinality(String),
    error_class     String,
    error_stage     LowCardinality(String),
    category        Enum8('TRANSIENT' = 1, 'PERMANENT' = 2, 'UNKNOWN' = 3),
    offset          UInt64,
    message_size    UInt32,
    inserted_at     DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(inserted_at)
ORDER BY (inserted_at, dlq_topic)
TTL inserted_at + INTERVAL 30 DAY;
```

**Bước 2 — bật DLQ ở từng sink connector:**
```json
"errors.tolerance": "all",
"errors.deadletterqueue.topic.name": "dlq.es-sink-transactions",
"errors.deadletterqueue.topic.replication.factor": "1",
"errors.deadletterqueue.context.headers.enable": "true"
```

Tên topic **phải khớp chính xác** danh sách `DLQ_TOPICS` hardcode trong Python — đây chính là metadata
sprawl #12 ở [`../architecture/BDP-current-state.md`](../architecture/BDP-current-state.md) §3.

> `errors.deadletterqueue.context.headers.enable` là **bắt buộc**. Thiếu nó thì header
> `__connect.errors.*` không được ghi, `parse_headers()` trả về rỗng, và **mọi** lỗi rơi vào nhóm
> `UNKNOWN` → không có replay nào xảy ra.

**Bước 3 — cân nhắc rủi ro của auto-replay.** Replay TRANSIENT gửi message về **topic gốc**, tức là
nó quay lại **mọi** consumer của topic đó, không riêng sink đã lỗi. Với `bankdb.public.transactions`,
điều đó nghĩa là Flink sẽ **đếm lại giao dịch đó** vào metric. Ở production nên replay vào topic retry
riêng, không phải topic gốc.

### 2.4 Kiểm tra

```bash
docker logs -f bigdata-dlq-processor        # in stats mỗi 30 giây
docker exec -it bigdata-kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic dlq.es-sink-transactions --from-beginning
```

Muốn ép ra lỗi để thử: dừng Elasticsearch (`docker compose stop elasticsearch`) trong khi CDC đang
chảy — ES sink sẽ lỗi và (nếu đã bật DLQ) đẩy message vào `dlq.es-sink-*`.

---

## 3. Fraud notifier

[`fraud_notifier.py`](../../fraud-notifier/fraud_notifier.py) consume `fraud-alerts` → gửi email SMTP.

| Cấu hình | Giá trị |
|---|---|
| Topic | `fraud-alerts` |
| `group_id` | `fraud-notifier-v2` |
| `auto_offset_reset` | `earliest` |
| SMTP | `SMTP_HOST`/`SMTP_PORT` từ `.env` (mặc định Gmail `smtp.gmail.com:587`, STARTTLS) |
| Gửi khi severity | `HIGH` hoặc `MEDIUM` |
| Cooldown | **300 giây mỗi account** — chống spam khi một account bắn liên tiếp |

**Phần chạy được:** đọc alert, format, gửi email — hoạt động nếu `.env` có `EMAIL_FROM`/`EMAIL_TO`/
`EMAIL_PASSWORD` hợp lệ (Gmail cần **App Password**, không dùng được mật khẩu thường).

**Phần không chạy:** `write_to_clickhouse()` INSERT vào `metrics.notification_events` — bảng không tồn
tại. Lỗi bị nuốt trong `try/except`, chỉ hiện trong log.

Tạo bảng nếu muốn ghi nhận lịch sử gửi:
```sql
CREATE TABLE IF NOT EXISTS metrics.notification_events (
    alert_type   LowCardinality(String),
    severity     LowCardinality(String),
    account_id   UInt64,
    action       LowCardinality(String),
    inserted_at  DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(inserted_at)
ORDER BY (inserted_at, account_id)
TTL inserted_at + INTERVAL 90 DAY;
```

> Cột phải khớp **chính xác** câu INSERT trong `write_to_clickhouse()` — đọc code trước khi tạo bảng.
> DDL trên dựng theo cách gọi hàm hiện tại, **chưa được kiểm chứng chạy thật**.

### 3.1 Kiểm tra

```bash
docker logs -f bigdata-fraud-notifier
```

Không thấy email? Đi ngược từng chặng:
1. Topic `fraud-alerts` có message không? (job Lane 3 đã submit chưa)
2. Alert có `severity` là `HIGH`/`MEDIUM` không? (`LOW` **không** gửi mail)
3. Account đó có đang trong cooldown 300 giây không?
4. SMTP có xác thực được không? → log sẽ có lỗi `login`

---

## 4. Vì sao ghi lại thay vì lặng lẽ sửa

Cả hai service là **code đúng, thiếu wiring**. Ghi lại rõ ràng có ích hơn xoá đi:

- Chúng cho thấy ý định thiết kế (phân loại lỗi, cooldown thông báo) — có giá trị tham khảo.
- Việc vá là **nhỏ và rõ**: 2 bảng + vài dòng config cho mỗi connector.
- Xoá đi rồi lại mất luôn phần thiết kế đã suy nghĩ kỹ.

Việc này nằm ở **Pha 0** của [lộ trình](../roadmap/BDP-metadata-driven-roadmap.md) — vá khoảng trống
chức năng **trước khi** tự động hoá, vì tự động hoá trên nền còn hỏng chỉ làm chỗ hỏng lan nhanh hơn.
