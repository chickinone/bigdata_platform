# ADR-0015: Metadata registry là YAML trong Git; generator sinh artifact, `check` gác cửa

- **Status:** Accepted
- **Date:** 2026-07-15
- **Deciders:** Phan Trường

## Bối cảnh

[ADR-0014](0014-adopt-metadata-driven-roadmap.md) chốt hướng metadata-driven. ADR này chốt **hình
dạng cụ thể** của bước đầu tiên.

Audit ở [`../architecture/BDP-current-state.md`](../architecture/BDP-current-state.md) §3 đo được:
schema của **một** thực thể bị khai lại bằng tay ở 13 nơi. Trước khi bỏ công mã hoá toàn bộ hệ thống
thành metadata, chúng tôi cần trả lời một câu hỏi rẻ tiền nhưng quyết định:

> **Contract có thật sự mang đủ thông tin để sinh lại chính xác artifact đang chạy không?**

Nếu câu trả lời là không, mọi kế hoạch phía sau đều sụp. Nên bước đầu phải là một **lát cắt dọc**
mỏng nhất có thể mà vẫn chứng minh được cả vòng lặp.

Chọn Elasticsearch sink làm phép thử vì nó có sẵn **oracle**: 5 file JSON viết tay đang chạy thật.
Sinh ra rồi so với chúng — khớp là mô hình đúng, lệch là mô hình thiếu.

Quan trọng hơn, 5 file đó **không đồng nhất**, nên phép thử không hề dễ dãi:

- 4 file CDC (`customers`, `accounts`, `transactions`, `transfers`): giống nhau từng dòng, chỉ khác
  `name`, `topics`, `transforms.extractKey.field`.
- `fraud-alerts` **khác hẳn cấu trúc**: `StringConverter`/`JsonConverter` thay Avro, `key.ignore=true`,
  `write.method=insert`, và **không có `transforms`** nào.

Một mô hình chỉ biết thay chuỗi sẽ vượt qua 4 file đầu và gãy ở file thứ 5.

## Quyết định

**1. Metadata là YAML trong Git, không phải registry service.** Đặt tại `metadata/datasets/`, một file
một thực thể. Git là nguồn sự thật; review bằng PR; không thêm hạ tầng nào.

**2. Contract mô tả *loại nguồn*, không chỉ tham số.** Trường `source.type` (`cdc_debezium` |
`app_json`) là thứ lái generator rẽ nhánh — đủ sức diễn tả cả `fraud-alerts` lẫn 4 bảng CDC bằng cùng
một mô hình.

**3. Cấu hình phụ thuộc nhau phải được *suy ra*, không khai riêng.** Ví dụ cốt lõi: `key.ignore`,
`write.method`, `behavior.on.null.values` không phải ba lựa chọn độc lập — chúng là hệ quả của **một**
sự thật "dataset này có khoá chính hay không":

```python
"key.ignore":  "false" if ds.primary_key else "true",
"write.method": "upsert" if ds.primary_key else "insert",
```

Viết tay thì ba trường đó có thể lệch nhau. Sinh ra thì **không thể** lệch.

**4. `check` là cửa gác, và nó so sánh ngữ nghĩa.** Lệnh `python -m dataplatform.cli check` sinh
artifact trong bộ nhớ rồi so với file trên đĩa, **trên dict đã parse — không so văn bản**. File viết
tay có dòng trống và thứ tự khoá do người sắp; ép generator tái tạo từng byte là vô nghĩa và giòn. Thứ
cần bảo toàn là ngữ nghĩa config — Kafka Connect đọc JSON, nó không quan tâm dòng trống.

**5. Chưa cắt chuyển.** Generator hiện chỉ *chứng minh* nó tái tạo được. File viết tay vẫn là bản đang
dùng. Việc xoá chúng thuộc bước sau, theo đúng chiến lược strangler-fig.

## Hệ quả

**Dễ hơn:**
- Đổi PK của một bảng = sửa một dòng contract; connector JSON tự đúng theo.
- Metadata sprawl #5 (PK bị chép tay vào JSON, tách rời định nghĩa bảng) bị xoá sổ **có bằng chứng**.
- `check` trả exit code 1 khi lệch → cắm thẳng vào CI được, không cần viết thêm gì.
- Thêm một stream `app_json` mới sẽ tự đi đúng nhánh generator, không phải sửa code.

**Khó hơn / phải chấp nhận:**
- Thêm phụ thuộc `pyyaml` + `jsonschema`.
- Thêm một lớp gián tiếp: đọc contract + generator thay vì đọc thẳng JSON. Người mới phải học control
  plane trước khi sửa được connector.
- Contract hiện **chưa** được đối chiếu ngược với schema thật trong Postgres/Schema Registry. Nó khớp
  với *artifact*, chưa chắc khớp với *database*. Đó là việc của Pha 1 (kiểm chứng ngược).
- `columns` hiện chưa generator nào dùng — nó tồn tại cho Flink/ClickHouse ở các pha sau. Chấp nhận có
  metadata "chưa sinh lợi" ngay, vì mã hoá lúc đọc schema một lần rẻ hơn quay lại đọc lần nữa.

## Kết quả kiểm chứng

```
Đối chiếu 5 artifact sinh từ metadata/ với file trên đĩa:
  [KHỚP] kafka-connect/es-sinks/es-sink-accounts.json
  [KHỚP] kafka-connect/es-sinks/es-sink-customers.json
  [KHỚP] kafka-connect/es-sinks/es-sink-fraud-alerts.json
  [KHỚP] kafka-connect/es-sinks/es-sink-transactions.json
  [KHỚP] kafka-connect/es-sinks/es-sink-transfers.json

KẾT QUẢ: 5/5 artifact khớp tuyệt đối.
```

Kèm **phép thử ngược** — vì một phép kiểm tra không bao giờ đỏ thì không chứng minh điều gì. Cố tình
đổi `primary_key` thành giá trị sai, `check` phải phát hiện:

```
  [KHÁC] kafka-connect/es-sinks/es-sink-transactions.json
          ~ transforms.extractKey.field: 'transaction_id' -> 'wrong_id'
KẾT QUẢ: 1/5 artifact lệch.  (exit code = 1)
```

Câu hỏi mở đầu đã có đáp án: **contract mang đủ thông tin.** Mô hình đứng vững.

## Phương án đã cân nhắc

- **Mã hoá toàn bộ metadata trước, sinh sau** (đúng thứ tự chữ trong roadmap Pha 1 → Pha 2). Bị loại:
  sẽ viết rất nhiều YAML dựa trên giả định chưa được kiểm chứng. Lát cắt dọc một-dataset-một-artifact
  trả lời cùng câu hỏi với chi phí nhỏ hơn nhiều. Roadmap §9 thật ra đã nói đúng điều này — chúng tôi
  làm theo §9, không theo thứ tự pha.
- **So sánh văn bản thay vì so dict.** Bị loại: buộc generator phải tái tạo cả dòng trống và thứ tự
  khoá của người viết. Giòn, và không bảo vệ thêm được gì.
- **Bắt đầu bằng Debezium connector.** Bị loại: nó gộp thông tin từ *nhiều* dataset
  (`table.include.list`), nên vừa phải chứng minh mô hình vừa phải giải bài toán tổng hợp. ES sink là
  một-dataset-một-file — tách bạch hơn.
- **Registry service (Postgres + REST) ngay từ đầu.** Bị loại: over-engineering. YAML + Git đủ cho quy
  mô 5 dataset, và Git đã cho sẵn version, review, rollback.
- **Sinh luôn cả `index template` cho ES** để sửa hạn chế `schema.ignore=true` (ADR-0011). Bị loại
  khỏi bước này: nó **thay đổi hành vi**, mà bước này cần diff rỗng để chứng minh tương đương. Trộn hai
  việc vào nhau sẽ mất chính cái oracle đang dùng. Xử lý riêng sau.
