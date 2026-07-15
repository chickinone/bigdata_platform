# Control plane — sinh artifact từ metadata

> Cách dùng `dataplatform/` để sinh config từ contract trong `metadata/`.
> Thiết kế: [ADR-0015](../decisions/0015-metadata-registry-yaml-first.md) ·
> Tên thư mục: [ADR-0016](../decisions/0016-rename-platform-to-dataplatform.md) ·
> Lộ trình: [`../roadmap/BDP-metadata-driven-roadmap.md`](../roadmap/BDP-metadata-driven-roadmap.md).
> Cập nhật lần cuối: 2026-07-15.

---

## 1. Ý tưởng trong một câu

> Khai báo mỗi thực thể **một lần** trong `metadata/datasets/*.yaml`, rồi để generator sinh ra mọi
> file cấu hình của thực thể đó — thay vì chép tay cùng thông tin sang từng công cụ.

**Trạng thái hiện tại:** mới phủ **Elasticsearch sink** (5/5 file). Các artifact khác (Debezium, S3
sink, DDL Flink/ClickHouse) vẫn viết tay — sẽ làm theo lộ trình.

---

## 2. Cài đặt

```bash
pip install pyyaml jsonschema
```

Chạy mọi lệnh ở **thư mục gốc repo** (nơi có `metadata/` và `dataplatform/`).

---

## 3. Ba lệnh

```bash
python -m dataplatform.cli check    # so bản sinh với file trên đĩa — KHÔNG ghi gì
python -m dataplatform.cli show     # in bản sinh ra màn hình — KHÔNG ghi gì
python -m dataplatform.cli write    # ghi bản sinh ĐÈ lên đĩa
```

| Lệnh | Đụng đĩa? | Dùng khi |
|---|---|---|
| `check` | Không | Mặc định. Xác nhận contract khớp hiện trạng. Exit 1 nếu lệch → dùng được trong CI. |
| `show` | Không | Xem generator sẽ sinh ra gì trước khi tin nó. |
| `write` | **Có** | Chỉ khi đã chủ đích cắt chuyển sang bản sinh. |

Kết quả `check` khi mọi thứ đúng:

```text
Đối chiếu 5 artifact sinh từ metadata/ với file trên đĩa:

  [KHỚP] kafka-connect/es-sinks/es-sink-accounts.json
  [KHỚP] kafka-connect/es-sinks/es-sink-customers.json
  [KHỚP] kafka-connect/es-sinks/es-sink-fraud-alerts.json
  [KHỚP] kafka-connect/es-sinks/es-sink-transactions.json
  [KHỚP] kafka-connect/es-sinks/es-sink-transfers.json

KẾT QUẢ: 5/5 artifact khớp tuyệt đối.
```

> `check` so **dict đã parse**, không so văn bản — dòng trống và thứ tự khoá không tính là lệch. Chỉ
> ngữ nghĩa config mới tính. Xem [ADR-0015](../decisions/0015-metadata-registry-yaml-first.md).

---

## 4. Cấu trúc

```text
metadata/
  datasets/
    oltp/      customers.yaml  accounts.yaml  transactions.yaml  transfers.yaml
    alerts/    fraud-alerts.yaml
dataplatform/
  registry.py            # đọc + validate contract (mọi generator đi qua đây)
  cli.py                 # check / write / show
  schemas/
    dataset.schema.json  # JSON Schema — định nghĩa "contract hợp lệ là gì"
  generators/
    es_sink.py           # contract -> config ES sink connector
```

Vì sao là `dataplatform/` chứ không `platform/`: tên sau trùng module chuẩn của Python và sẽ che mất
stdlib. Xem [ADR-0016](../decisions/0016-rename-platform-to-dataplatform.md).

---

## 5. Thêm một dataset mới

1. Tạo `metadata/datasets/<layer>/<tên>.yaml`. Chép một file có sẵn làm khuôn.
2. `python -m dataplatform.cli check` → sẽ báo `[MỚI]` vì chưa có file trên đĩa.
3. `python -m dataplatform.cli show` → đọc kỹ bản sinh.
4. `python -m dataplatform.cli write` → ghi ra.
5. Đăng ký connector: xem [`cdc-and-connectors.md`](cdc-and-connectors.md).

**Đổi PK của một bảng** giờ chỉ là sửa một dòng `primary_key:` — `transforms.extractKey.field` tự
đúng theo. Trước đây phải nhớ sửa tay trong JSON, và sai thì upsert hỏng âm thầm (ADR-0011).

---

## 6. Contract nói gì

Các trường quan trọng — đầy đủ ở [`dataset.schema.json`](../../dataplatform/schemas/dataset.schema.json):

| Trường | Vai trò |
|---|---|
| `urn` | Định danh duy nhất. Quy ước `<domain>.<schema>.<table>`. |
| `source.type` | **Lái generator rẽ nhánh.** `cdc_debezium` → Avro + `unwrap` + `extractKey`. `app_json` → JSON trần, không transform. |
| `source.topic` | Tên topic Kafka. |
| `source.replica_identity` | `full` cho bảng cần audit thay đổi (ADR-0004). |
| `primary_key` | **Có** → upsert được (`key.ignore=false`, `write.method=upsert`). **Không có** → chỉ append. |
| `columns[].encoded_as` | `string` cho cột decimal — hệ quả của `decimal.handling.mode=string` (ADR-0003). Generator dựa vào đây để chèn CAST. |
| `columns[].pii` | Đánh dấu PII để catalog/lineage sau này trả lời được "PII nằm ở đâu". |
| `sinks.*.enabled` | Dataset này cần đổ đi đâu. |

**Không khai `key.ignore` hay `write.method` trong contract** — chúng được **suy ra** từ việc có
`primary_key` hay không. Đó là điểm mấu chốt: những thứ phụ thuộc nhau thì không được phép khai rời,
vì khai rời là cho phép chúng lệch nhau.

---

## 7. Giới hạn hiện tại

| Giới hạn | Ghi chú |
|---|---|
| Chỉ sinh ES sink | Debezium, S3 sink, DDL Flink/ClickHouse vẫn viết tay. |
| Chưa cắt chuyển | File viết tay vẫn là bản đang dùng; generator mới chỉ *chứng minh* tái tạo được. |
| Contract chưa đối chiếu ngược với DB thật | Nó khớp *artifact*, chưa chắc khớp *schema Postgres*. Việc của Pha 1. |
| `columns` chưa được dùng | Tồn tại sẵn cho Flink/ClickHouse ở pha sau. |
| Chưa có CI | `check` chạy tay. Cắm vào CI là việc kế tiếp. |
