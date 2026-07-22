# ADR-0012: DLQ processor tồn tại nhưng chưa được nối — ghi lại thay vì xoá

- **Status:** **Superseded by [ADR-0017](0017-dlq-flow-observe-then-park.md)** *(2026-07-16 — DLQ đã được nối)*
- **Date:** 2026-07-15 *(hồi tố)*
- **Deciders:** Phan Trường

> **Cập nhật 2026-07-16:** nợ kỹ thuật trong ADR này **đã được trả** cho phần DLQ —
> [ADR-0017](0017-dlq-flow-observe-then-park.md) nối dây đầy đủ và sửa lỗi replay nguy hiểm mà ADR này
> cảnh báo. Giữ lại ADR này vì nó ghi *vì sao* chuyện đó tồn tại lâu mà không lộ.
> Phần `fraud-notifier` / `metrics.notification_events` **vẫn còn nguyên** — mục 3 dưới đây vẫn đúng.

## Bối cảnh

Repo có service `dlq-processor` với logic phân loại lỗi khá chỉn chu, và `fraud-notifier` gửi email
cảnh báo. Cả hai đều chạy trong compose và **trông như đang hoạt động**.

Đọc code cho thấy không phải vậy:

```bash
$ grep -rl "deadletterqueue" kafka-connect/ debezium/
# không kết quả — không connector nào bật DLQ

$ grep -c "dlq_events\|notification_events" clickhouse/init/01_schema.sql
0                                            # không bảng đích nào tồn tại
```

Ba sự thật cùng lúc:
1. `dlq-processor` subscribe 6 topic `dlq.*` **không bao giờ có message** — không producer nào cả.
2. Nó INSERT vào `metrics.dlq_events` — bảng **không tồn tại**.
3. `fraud-notifier` INSERT vào `metrics.notification_events` — cũng **không tồn tại**.

Vì sao chuyện này tồn tại lâu mà không lộ — ba lớp che phối hợp với nhau:
- `AUTO_CREATE_TOPICS_ENABLE=true` ([ADR-0005](0005-kafka-kraft-single-node.md)) **tạo rỗng** 6 topic
  `dlq.*` khi consumer subscribe → service log "Connected to Kafka, monitoring 6 topics" và trông khoẻ.
- Cả hai service bọc lệnh ghi ClickHouse trong `try/except` và chỉ `log.error(...)` → **không crash**.
- Không có health check nào kiểm tra chúng làm được việc hay không.

## Quyết định

**Ghi lại tình trạng này rõ ràng trong tài liệu, không xoá code và không lặng lẽ vá.** Đánh dấu chúng
là "code đúng, thiếu wiring" ở [`../guide/dlq-and-notifier.md`](../guide/dlq-and-notifier.md), kèm
đúng các bước cần để nối. Việc vá thuộc **Pha 0** của
[lộ trình](../roadmap/BDP-metadata-driven-roadmap.md).

Mọi sơ đồ kiến trúc vẽ đường `dlq.*` bằng **nét đứt** để không tuyên bố sai về hệ thống.

## Hệ quả

**Dễ hơn:**
- Người đọc không mất thời gian debug một service "hỏng" mà thật ra chưa từng được nối.
- Ý định thiết kế (phân loại TRANSIENT/PERMANENT/UNKNOWN, cooldown 300 giây/account) được giữ lại —
  đó là phần đã suy nghĩ kỹ, có giá trị tham khảo.
- Việc vá là nhỏ và rõ: 2 bảng + vài dòng config mỗi connector.

**Khó hơn / phải chấp nhận:**
- Repo giữ code không chạy → phải có tài liệu bù, nếu không nó lại đánh lừa người sau.
- Trong lúc chờ, **lỗi connector im lặng biến mất**. Không DLQ nghĩa là message hỏng bị bỏ qua
  (`errors.tolerance` mặc định là `none` → task **fail** thay vì bỏ message; nhưng lỗi vẫn không được
  ghi lại ở đâu để phân tích).

### Cần gì để nối

1. Tạo `metrics.dlq_events` và `metrics.notification_events` (DDL ở
   [`../guide/dlq-and-notifier.md`](../guide/dlq-and-notifier.md)).
2. Thêm vào mỗi sink connector:
   ```json
   "errors.tolerance": "all",
   "errors.deadletterqueue.topic.name": "dlq.es-sink-transactions",
   "errors.deadletterqueue.topic.replication.factor": "1",
   "errors.deadletterqueue.context.headers.enable": "true"
   ```
   `context.headers.enable` là **bắt buộc** — thiếu nó, `parse_headers()` trả rỗng và **mọi** lỗi rơi
   vào `UNKNOWN`, không có replay nào xảy ra.
3. Tên topic phải khớp **chính xác** danh sách `DLQ_TOPICS` hardcode trong Python (metadata sprawl #12).

### Một cảnh báo về auto-replay

Logic replay TRANSIENT gửi message về **topic gốc**. Với `bankdb.public.transactions`, message đó quay
lại **mọi** consumer của topic — kể cả Flink, nghĩa là giao dịch bị **đếm lại** vào metric.

Nói cách khác: bật DLQ nguyên trạng sẽ khiến một lỗi ES tạm thời làm **sai số liệu dashboard**. Ở
production phải replay vào topic retry riêng, không phải topic gốc. Phải sửa **trước** khi bật DLQ.

## Phương án đã cân nhắc

- **Xoá cả hai service.** Bị loại: mất phần thiết kế đã suy nghĩ kỹ, và mất luôn khả năng quan sát lỗi
  vốn đã thiếu.
- **Vá ngay trong lần thay đổi này.** Bị loại: cần đổi 6 file connector + DDL ClickHouse + sửa logic
  replay, và phải kiểm chứng chạy thật. Đó là một thay đổi riêng, không phải việc kèm khi viết tài liệu.
- **Để im, không ghi gì.** Bị loại: đây chính là cách nó tồn tại lâu đến vậy.
