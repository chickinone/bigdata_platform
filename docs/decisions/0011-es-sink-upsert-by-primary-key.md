# ADR-0011: ES sink upsert theo PK, `schema.ignore=true`

- **Status:** Accepted *(phần `schema.ignore` nên xem lại)*
- **Date:** 2026-07-15 *(hồi tố)*
- **Deciders:** Phan Trường

## Bối cảnh

Elasticsearch phục vụ tra cứu: tìm một khách hàng, xem giao dịch lỗi gần nhất, theo dõi lifecycle
chuyển tiền. Nó cần phản ánh **trạng thái hiện tại**, không phải một CDC log chỉ biết thêm mới.

Nếu mỗi event CDC tạo một document mới, thì một account được cập nhật 50 lần sẽ thành 50 document —
tra cứu vô nghĩa.

## Quyết định

Cấu hình 5 ES sink connector để **upsert theo khoá chính**, dùng `unwrap` + `extractKey`:

```json
"transforms": "unwrap,extractKey",
"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
"transforms.extractKey.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
"transforms.extractKey.field": "transaction_id",
"key.ignore": "false",
"schema.ignore": "true",
"write.method": "upsert",
"behavior.on.null.values": "delete"
```

Mỗi thực thể một connector, `extractKey.field` là PK của thực thể đó.

## Hệ quả

**Dễ hơn:**
- Document ES phản ánh trạng thái hiện tại; một account = một document dù cập nhật bao nhiêu lần.
- `_id` = PK nghiệp vụ → tra cứu theo id trực tiếp.
- `behavior.on.null.values=delete` → xoá ở nguồn kéo theo xoá document.

**Khó hơn / phải chấp nhận:**
- **`extractKey.field` sai = upsert hỏng âm thầm.** Mỗi event thành document mới, index phình ra, tra
  cứu sai — mà không có lỗi nào. Đây là metadata sprawl #5 ở
  [`../architecture/BDP-current-state.md`](../architecture/BDP-current-state.md) §3: PK bị khai báo lại
  bằng tay trong JSON, tách rời khỏi định nghĩa bảng.
- **5 file JSON gần như giống hệt nhau**, khác đúng 2 dòng (`topics`, `extractKey.field`). Pha 2 của
  [lộ trình](../roadmap/BDP-metadata-driven-roadmap.md) sinh cả 5 từ danh sách entity.
- `es-sink-fraud-alerts` **không** có `extractKey` (alert không có PK tự nhiên) → ES tự sinh `_id`,
  nên alert là append-only. Đúng ý định, nhưng lệch khỏi khuôn của 4 cái kia.

### `schema.ignore=true` — nên xem lại

Cấu hình này bảo ES **tự đoán** mapping từ document đầu tiên thay vì lấy từ Avro schema. Nó tránh được
lỗi khi Connect dịch schema Avro sang mapping ES, nhưng gây ra hai vấn đề thật:

**(a) Cột số bị map thành chuỗi.** Vì `decimal.handling.mode=string` ([ADR-0003](0003-avro-with-schema-registry.md)),
`amount`/`balance` đến ES dưới dạng chuỗi, và ES map chúng thành `text`/`keyword`. Nên:
```text
balance > 10000        ← không chạy
status: "frozen"       ← phải dùng cách này
```

**(b) Trường thời gian của alert không phải date.** Alert dùng epoch millis dạng số, Kibana không nhận
là date → data view `fraud-alerts` không chọn được time field, không vẽ được date histogram.

**Đây là hai quyết định cộng hưởng thành một hạn chế lớn hơn tổng của chúng.** Mỗi cái riêng lẻ đều
hợp lý; cộng lại thì tiền tệ trong ES thành không lọc được theo số.

**Cách sửa:** khai báo **index template** trong ES **trước khi** sink chạy lần đầu, ép kiểu cho các
trường này. Mapping **không** đổi được sau khi index đã tạo — phải reindex. Chưa làm.

## Phương án đã cân nhắc

- **`schema.ignore=false`** (lấy mapping từ Avro). Bị loại lúc đầu vì lỗi dịch schema, nhất là với
  decimal-dạng-string và timestamp. **Nên thử lại** cùng với index template.
- **`key.ignore=true`.** Bị loại: ES tự sinh `_id` → không upsert được, mỗi event một document.
- **Một connector cho tất cả topic.** Bị loại: `extractKey.field` là **per-connector**, mà mỗi thực
  thể có tên PK khác nhau. Không có cách khai báo một field khác nhau theo từng topic trong một
  connector.
- **Ghi ES từ Flink.** Bị loại: Kafka Connect sinh ra đúng cho việc đổ dữ liệu; giữ Flink cho tính
  toán ([ADR-0006](0006-one-flink-job-per-lane-statement-set.md)).
