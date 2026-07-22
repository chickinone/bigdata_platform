# ADR-0017: Nối DLQ — quan sát trước, phát lại sau; đi qua Kafka, không INSERT thẳng

- **Status:** Accepted
- **Date:** 2026-07-16
- **Deciders:** Phan Trường
- **Supersedes:** [ADR-0012](0012-dlq-processor-not-wired.md) (ghi nhận nợ kỹ thuật; nay đã trả)

## Bối cảnh

[ADR-0012](0012-dlq-processor-not-wired.md) ghi lại rằng DLQ là **code đúng nhưng chưa nối dây**: không
connector nào bật `errors.deadletterqueue.*`, và hai bảng đích (`metrics.dlq_events`,
`metrics.notification_events`) không tồn tại. Hệ quả: lỗi của Kafka Connect biến mất không dấu vết.

Khi bắt tay nối, đọc kỹ `dlq_processor.py` thì lộ ra vấn đề lớn hơn "thiếu config". Dòng xử lý
TRANSIENT có **ba lỗi chồng nhau**:

```python
producer.send(original_topic, value=msg.value, headers=msg.headers)
```

| # | Lỗi | Hậu quả |
|---|---|---|
| 1 | Replay về **topic gốc** | `bankdb.public.transactions` cũng là nguồn của Flink Lane 1 và Lane 3 → giao dịch **bị đếm lại** → sai số liệu dashboard. Một lỗi ES tạm thời làm hỏng dữ liệu metric. |
| 2 | **Mất `key`** (không truyền `key=msg.key`) | ES sink dùng `extractKey` lấy PK từ key làm `_id`. Không key → fail lại → quay về DLQ → **vòng lặp vô tận**. |
| 3 | Không giới hạn số lần thử | ES sập một tiếng → replay quay vòng liên tục. |

Nói cách khác: **bật DLQ nguyên trạng sẽ chủ động làm hỏng dữ liệu.** Đây là lý do ADR-0012 ghi rõ
"phải sửa trước khi bật DLQ" thay vì bật cho xong.

## Quyết định

**1. Bật DLQ cho mọi sink — chính sách nền tảng, không phải lựa chọn từng dataset.**

```json
"errors.tolerance": "all",
"errors.deadletterqueue.topic.name": "dlq.<connector>",
"errors.deadletterqueue.topic.replication.factor": "1",
"errors.deadletterqueue.context.headers.enable": "true",
"errors.log.enable": "true",
"errors.log.include.messages": "false"
```

`errors.tolerance: all` chuyển bản ghi lỗi sang DLQ thay vì để task chết. Mặc định `none` khiến một
message hỏng làm đứng toàn bộ connector. `all` chỉ an toàn **khi** có DLQ và có người nhìn — nay cả
hai điều kiện đều có.

`context.headers.enable` là **bắt buộc**: thiếu nó thì header `__connect.errors.*` không được ghi,
mọi lỗi rơi vào nhóm `UNKNOWN`, và việc phân loại thành vô nghĩa.

`errors.log.include.messages` **cố ý để `false`**: nó in nội dung bản ghi ra log Connect, mà
`customers` chứa `full_name`/`email`/`phone`. Log không phải chỗ cho PII. Nội dung gốc vẫn nằm nguyên
trong topic DLQ nếu cần điều tra.

**2. Danh sách topic DLQ được sinh từ metadata, không hardcode.**

Trước đây 6 topic `dlq.*` nằm cứng trong `dlq_processor.py`, tách rời khỏi cấu hình connector — thêm
connector mà quên sửa list Python thì lỗi của nó rơi vào hư không (metadata sprawl #12). Nay
`dlq-processor/dlq_topics.json` được sinh từ `metadata/datasets/*.yaml`, đi kèm ADR-0015.

**3. DLQ event đi qua Kafka, không INSERT thẳng ClickHouse.**

```text
sink lỗi → dlq.<connector> → dlq-processor → dlq.events (Kafka)
         → metrics.dlq_events_kafka → _mv → metrics.dlq_events → Grafana
```

Ba lý do, lý do thứ hai là quyết định:

1. Đúng pattern đã chốt ở [ADR-0007](0007-clickhouse-kafka-engine-serving.md) — Flink cũng không ghi
   thẳng ClickHouse.
2. **DLQ event là thứ ta cần nhất đúng lúc hệ thống đang hỏng.** Nếu ClickHouse cũng đang sập mà
   processor INSERT thẳng HTTP, ta **mất bản ghi lỗi ngay tại thời điểm cần nó nhất**. Qua Kafka thì
   chúng nằm chờ; ClickHouse lên là đuổi kịp.
3. ClickHouse ghét insert nhỏ lẻ; Kafka engine gom thành block lớn.

**4. Không tự động phát lại. Mọi nhóm đều `PARKED`.**

Ghi nhận đầy đủ rồi dừng, chờ người xử lý. Đích phát lại **đúng** phải là một topic chỉ connector đó
đọc — topic gốc không thoả vì Flink cũng đọc. Xây topic retry riêng thì vướng: ES sink lấy **tên
index** từ tên topic, còn S3 sink lấy **đường dẫn partition** từ tên topic, nên mỗi connector cần thêm
`RegexRouter` riêng. Đó là việc riêng; làm nửa vời còn tệ hơn không làm.

Trong lúc chờ, mất mát ít hơn tưởng: Kafka Connect đã tự retry vài lần **trước khi** đẩy vào DLQ, nên
phần lớn lỗi thoáng qua đã được xử lý trước khi tới đây.

**5. Lưu hai vị trí, không trộn làm một.**

Phát hiện khi kiểm chứng: `msg.offset` là vị trí trong **topic DLQ**, còn header
`__connect.errors.offset` là vị trí trong **topic gốc**. Bản đầu tiên chỉ lưu cái thứ nhất — tức là
"PARKED chờ người xử lý" thành lời hứa suông, vì không có đường tìm lại bản ghi lỗi.

| Cột | Ý nghĩa |
|---|---|
| `dlq_partition`, `dlq_offset` | Vị trí trong topic DLQ → **khoá chống trùng** |
| `original_partition`, `original_offset` | Vị trí trong topic gốc → **để tìm lại mà phát lại** |

**6. Bảng dùng `ReplacingMergeTree` khoá theo `(dlq_topic, dlq_partition, dlq_offset)`.**

Bộ ba đó là khoá tự nhiên duy nhất của một message Kafka. Processor là at-least-once, nên restart có
thể đọc lại — dedup làm việc đó vô hại.

**7. Không nhân bản nội dung message sang ClickHouse.** Chỉ lưu `message_key`. Nội dung gốc đã nằm
trong topic DLQ; chép nó sang ClickHouse là nhân bản PII ra thêm một chỗ, giữ 30 ngày, không thêm giá
trị điều tra nào.

## Hệ quả

**Dễ hơn:**
- Lỗi connector từ chỗ **biến mất không dấu vết** thành **dữ liệu truy vấn được** bằng SQL, có Grafana.
- Phân biệt được lỗi hạ tầng (TRANSIENT) với lỗi dữ liệu (PERMANENT) — hai thứ cần cách xử lý khác hẳn.
- Một message hỏng không còn làm chết cả connector (`errors.tolerance: all`).
- Thêm connector mới → topic DLQ tự vào bản kê, không ai phải nhớ.

**Khó hơn / phải chấp nhận:**
- **`errors.tolerance: all` chỉ an toàn khi có người nhìn.** Nó biến "task chết ồn ào" thành "bản ghi
  lặng lẽ sang DLQ". Nếu không ai xem `metrics.dlq_events`, đây là **bước lùi** so với fail-fast. Việc
  còn nợ: dashboard Grafana + cảnh báo khi DLQ tăng đột biến.
- Phát lại vẫn là việc **thủ công** — chưa có công cụ, mới có đủ thông tin (`original_offset`) để làm.
- Thêm 3 đối tượng ClickHouse và 1 topic Kafka.
- `metrics.notification_events` (fraud-notifier) **vẫn chưa tồn tại** — cùng loại lỗi, nằm ngoài phạm
  vi lần này. ADR-0012 vẫn đúng ở điểm đó.

## Kết quả kiểm chứng

Chạy thật trên stack (Kafka + ClickHouse + dlq-processor), giả lập bản ghi DLQ đúng như Kafka Connect
sinh ra, kèm header `__connect.errors.*`:

```text
DLQ: connector=es-sink-transactions topic_goc=bankdb.public.transactions
     stage=VALUE_CONVERTER loi=...DataException nhom=PERMANENT -> PARKED
DLQ: connector=s3-sink-cdc topic_goc=bankdb.public.accounts
     stage=TASK_PUT loi=java.net.ConnectException nhom=TRANSIENT -> PARKED
```

Dữ liệu tới `metrics.dlq_events`, hai vị trí tách bạch đúng:

```text
connector_name:     es-sink-transactions
category:           PERMANENT        action: PARKED
dlq_partition:      0    dlq_offset:      1     <- trong topic DLQ
original_partition: 2    original_offset: 99    <- trong topic gốc
```

Chống trùng: gửi lại **cùng** một bản ghi 3 lần (giả lập processor restart đọc lại) → sau
`OPTIMIZE FINAL` còn **1 dòng**. Idempotent như thiết kế.

## Phương án đã cân nhắc

- **Giữ auto-replay về topic gốc.** Bị loại: làm sai số liệu metric (Flink đếm lại). Đây không phải
  rủi ro lý thuyết — `bankdb.public.transactions` có đúng 3 consumer độc lập.
- **Replay về topic retry riêng ngay lần này.** Bị hoãn: cần `RegexRouter` riêng cho từng connector vì
  ES lấy tên index và S3 lấy đường dẫn partition từ tên topic. Đủ phức tạp để xứng một thay đổi riêng
  có kiểm chứng riêng.
- **Giữ INSERT thẳng ClickHouse qua HTTP** (như code cũ). Bị loại: mất DLQ event khi ClickHouse sập —
  đúng lúc cần nhất. Và trái pattern của chính dự án (ADR-0007).
- **`errors.tolerance: none`** (fail-fast, không DLQ). Bị loại: một bản ghi hỏng làm đứng toàn bộ luồng
  dữ liệu của cả một thực thể. Với `transactions` ở 150–800 RPS thì đó là sự cố, không phải an toàn.
- **Lưu cả nội dung message vào ClickHouse.** Bị loại: nhân bản PII thêm một chỗ mà không thêm giá trị
  — nội dung gốc vẫn ở topic DLQ.
- **Xoá `dlq-processor` cho gọn.** Bị loại: đó là bỏ luôn khả năng quan sát lỗi vốn đã thiếu.
