# ADR-0007: ClickHouse serving qua Kafka engine + Materialized View

- **Status:** Accepted
- **Date:** 2026-07-15 *(hồi tố)*
- **Deciders:** Phan Trường

## Bối cảnh

Metric của Flink phải đến được Grafana. Cách hiển nhiên là cho Flink ghi thẳng vào ClickHouse bằng
JDBC sink. Câu hỏi là: nên đi thẳng, hay đi vòng qua Kafka?

## Quyết định

Flink ghi metric ra **Kafka topic** `metrics.*` dạng JSON. ClickHouse **tự kéo về** bằng bảng Kafka
engine + Materialized View đổ vào bảng `ReplacingMergeTree`.

```text
Flink → Kafka metrics.timeseries → metrics.timeseries_kafka (Kafka engine)
                                 → metrics.timeseries_mv (MV)
                                 → metrics.timeseries (ReplacingMergeTree + TTL)
```

Ba đối tượng cho mỗi metric, 4 metric → 12 bảng.

## Hệ quả

**Dễ hơn:**
- **ClickHouse sập không làm Flink backpressure.** Message nằm chờ trong Kafka; ClickHouse lên là
  đuổi kịp. Ghi thẳng JDBC thì ClickHouse chậm sẽ dội ngược lên toàn bộ job Flink.
- **Metric replay được.** Xoá bảng ClickHouse, đổi `kafka_group_name`, tạo lại MV — nó đọc lại topic.
- **Consumer khác cắm thêm được** vào `metrics.*` mà không đụng Flink.
- ClickHouse tự quản lý batch (`kafka_max_block_size = 1048576`) — insert lớn, hiệu quả, đúng thứ
  MergeTree thích.

**Khó hơn / phải chấp nhận:**
- Thêm một chặng → độ trễ cao hơn ghi thẳng. Với dashboard thì không đáng kể.
- **Schema phải khớp thủ công ở hai đầu**: sink DDL trong Flink và bảng Kafka engine trong ClickHouse.
  Lệch cột → **MV bỏ dữ liệu không báo lỗi**. Đây là chế độ hỏng tệ nhất của cả hệ thống: dashboard
  vẫn xanh, chỉ là rỗng.
- **12 khối schema viết tay** phải khớp tuyệt đối (4 metric × 3 đối tượng), cộng 4 sink DDL trong
  Flink = 16 khối cho 4 metric. Đây là metadata sprawl #8 và #9 ở
  [`../architecture/BDP-current-state.md`](../architecture/BDP-current-state.md) §3.
- **Bảng Kafka engine đọc một lần là mất.** `SELECT` vào `<m>_kafka` sẽ *tiêu thụ* message và cướp dữ
  liệu của MV. Bẫy khi debug.
- **Thứ tự khởi tạo quan trọng.** Kafka engine bắt đầu đọc từ lúc bảng được tạo. Tạo MV sau khi Flink
  đã bơm metric một lúc → phần trước đó mất.

[Pha 4 của lộ trình](../roadmap/BDP-metadata-driven-roadmap.md) sinh **cả** sink DDL của Flink **và**
3 khối ClickHouse từ **một** spec metric, đúng để triệt cái "khớp thủ công" này.

## Chi tiết engine

`ReplacingMergeTree(inserted_at)` chứ không phải `MergeTree` thường — vì sink của Flink là
`AT_LEAST_ONCE`, cùng một cửa sổ có thể được phát lại sau restart. ReplacingMergeTree gộp bản trùng
theo `ORDER BY` lúc merge nền.

> **Lưu ý:** gộp xảy ra lúc **merge**, không phải lúc **query**. Query có thể thấy bản trùng cho tới
> khi merge chạy. Cần chính xác tuyệt đối thì dùng `FINAL` (chậm) hoặc `argMax(...)`.

TTL: `timeseries`/`breakdown`/`topn` 30 ngày, `kpi` **90 ngày** (ít dòng nhất vì không group by).
ClickHouse **không** là nguồn sự thật — lưu trữ dài hạn nằm ở lakehouse.

## Phương án đã cân nhắc

- **Flink JDBC sink ghi thẳng ClickHouse.** Bị loại: ClickHouse chậm/sập là backpressure ngược lên
  Flink; không replay được; ClickHouse ghét insert nhỏ lẻ mà JDBC sink hay tạo ra.
- **Flink ghi thẳng, Kafka chỉ để backup.** Bị loại: hai đường ghi, hai chỗ có thể lệch.
- **`MergeTree` thường.** Bị loại: sink AT_LEAST_ONCE sẽ để lại bản trùng vĩnh viễn.
- **Định dạng Avro cho topic `metrics.*`.** Bị loại: `JSONEachRow` của ClickHouse Kafka engine đơn giản
  và đủ dùng; metric là dữ liệu nội bộ, không cần tiến hoá schema như CDC. (Đánh đổi: mất kiểm tra kiểu
  — đúng chỗ khiến lệch cột trở nên âm thầm.)
