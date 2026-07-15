# ADR-0003: Avro + Schema Registry cho CDC; `decimal.handling.mode=string`

- **Status:** Accepted
- **Date:** 2026-07-15 *(hồi tố)*
- **Deciders:** Phan Trường

## Bối cảnh

Message CDC cần một định dạng nối được nhiều consumer khác nhau (Flink, ES sink, S3 sink) và không
nhét schema vào từng message.

Riêng cột `NUMERIC` là một bài toán thật: Avro biểu diễn decimal dưới dạng logical type
`bytes` + scale/precision. Nhiều consumer decode ra `ByteBuffer` thô hoặc số bị sai scale âm thầm —
với dữ liệu tiền tệ thì đó là lỗi không chấp nhận được.

## Quyết định

Dùng **Confluent Avro converter + Schema Registry** cho cả key và value của CDC, và đặt
`decimal.handling.mode = string` để mọi cột `NUMERIC` được mã hoá thành **STRING**.

```json
"key.converter": "io.confluent.connect.avro.AvroConverter",
"value.converter": "io.confluent.connect.avro.AvroConverter",
"value.converter.schema.registry.url": "${env:SCHEMA_REGISTRY_URL}",
"decimal.handling.mode": "string",
"time.precision.mode": "adaptive"
```

## Hệ quả

**Dễ hơn:**
- Message nhỏ (schema tra theo ID, không lặp trong từng message).
- Schema có nơi tập trung, Kafka UI decode Avro hiển thị được.
- **Không mất chính xác** cho tiền tệ: `"1234.5600"` là chuỗi thập phân chính xác, không có
  floating-point làm tròn, không có lỗi scale.

**Khó hơn / phải chấp nhận:**
- **Mọi consumer phải tự cast.** Đây là cái giá lan ra khắp hệ thống:
  - Flink: `SUM(CAST(after.amount AS DECIMAL(19,4)))` — ở mọi aggregation
  - Spark: `col("amount").cast("double")` — trong `build_gold_layer.py`
  - Elasticsearch: `amount`/`balance` bị map thành `text`/`keyword` (do `schema.ignore=true`), nên
    filter dạng số **không hoạt động** — xem [`../guide/kibana.md`](../guide/kibana.md) §2
  - Bronze Parquet: lưu string, để Silver/Gold cast
- Quên cast là so sánh chuỗi: `"9" > "10"` cho ra `true`.
- Schema Registry thành **điểm phụ thuộc bắt buộc** — nó sập thì cả producer lẫn consumer đứng.
- Đã có Schema Registry nhưng **chưa bật compatibility gate** — breaking change vẫn lọt qua. Việc này
  nằm ở [Pha 7 của lộ trình](../roadmap/BDP-metadata-driven-roadmap.md).

Contract metadata trong [lộ trình](../roadmap/BDP-metadata-driven-roadmap.md) ghi lại chuyện này bằng
trường `encoded_as: string` bên cạnh `type: decimal(19,4)`, để generator biết chèn CAST vào đúng chỗ
thay vì trông chờ con người nhớ.

## Phương án đã cân nhắc

- **`decimal.handling.mode=precise` (mặc định).** Bị loại: cho ra Avro `bytes` + scale. Consumer decode
  thành `ByteBuffer` và phải tự dựng lại `BigDecimal` bằng đúng scale. PyFlink và Spark xử lý cái này
  không đồng nhất; sai scale làm số tiền lệch 100 hoặc 10000 lần mà **không** báo lỗi.
- **`decimal.handling.mode=double`.** Bị loại: mất chính xác. Không bao giờ dùng floating-point cho
  tiền.
- **JSON converter.** Bị loại: message phình to (schema lặp trong từng message hoặc mất hẳn kiểu), và
  không có tiến hoá schema tập trung.
- **Protobuf.** Bị loại: Debezium hỗ trợ được, nhưng hệ sinh thái Flink/ClickHouse của stack này quen
  Avro hơn.
