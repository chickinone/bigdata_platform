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

**Trạng thái hiện tại — 11 artifact:**

| Artifact | Số file | Ghi chú |
|---|---|---|
| ES sink connector | 5 | 1 dataset → 1 connector |
| S3 sink connector (Bronze) | 1 | N dataset → 1 connector (gộp `topics`) |
| Debezium source connector | 1 | N dataset → 1 (gộp `table.include.list`) — [ADR-0018](../decisions/0018-generate-debezium-and-publication.md) |
| Publication SQL | 1 | **text/SQL**, cùng nguồn với Debezium → không thể lệch |
| DDL ClickHouse | 2 | 12 đối tượng metric từ `columns` — [ADR-0019](../decisions/0019-generate-clickhouse-metric-ddl.md) |
| Bản kê topic DLQ | 1 | `dlq-processor/dlq_topics.json` ([ADR-0017](../decisions/0017-dlq-flow-observe-then-park.md)) |

Cấu hình DLQ của **cả 6 connector** cũng sinh từ đây. Còn viết tay: DDL sink bên Flink
(`lane1_dashboard.py`), job Spark, catalog Trino — sẽ làm theo lộ trình.

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
Đối chiếu 11 artifact sinh từ metadata/ với file trên đĩa:

  [KHỚP] clickhouse/init/01_schema.sql
  [KHỚP] clickhouse/init/02_kafka_consumers.sql
  [KHỚP] debezium/postgres-connector.json
  [KHỚP] dlq-processor/dlq_topics.json
  [KHỚP] kafka-connect/es-sinks/es-sink-accounts.json
  [KHỚP] kafka-connect/es-sinks/es-sink-customers.json
  [KHỚP] kafka-connect/es-sinks/es-sink-fraud-alerts.json
  [KHỚP] kafka-connect/es-sinks/es-sink-transactions.json
  [KHỚP] kafka-connect/es-sinks/es-sink-transfers.json
  [KHỚP] kafka-connect/s3-sinks/s3-sink-cdc.json
  [KHỚP] postgres/init/04_publication.sql

KẾT QUẢ: 11/11 artifact khớp tuyệt đối.
Contract mang đủ thông tin để sinh lại toàn bộ file viết tay.
```

> `check` so **dict đã parse**, không so văn bản — dòng trống và thứ tự khoá không tính là lệch. Chỉ
> ngữ nghĩa config mới tính. Xem [ADR-0015](../decisions/0015-metadata-registry-yaml-first.md).

Nó còn biết khoá nào **bất biến theo thứ tự**: `topics` là danh sách ngăn phẩy nhưng Kafka Connect coi
đó là một *tập hợp*, nên `check` so nó như set. Không có điều đó, generator sắp `topics` khác người
viết tay sẽ báo lệch giả.

---

## 4. Cấu trúc

```text
metadata/
  datasets/
    oltp/      customers.yaml  accounts.yaml  transactions.yaml  transfers.yaml
    metrics/   timeseries.yaml  kpi.yaml  breakdown.yaml  topn.yaml
    alerts/    fraud-alerts.yaml
dataplatform/
  registry.py            # đọc + validate contract (mọi generator đi qua đây)
  cli.py                 # check / write / show
  schemas/
    dataset.schema.json  # JSON Schema — định nghĩa "contract hợp lệ là gì"
  generators/
    es_sink.py               # 1 dataset  -> 1 ES sink connector
    s3_sink.py               # N dataset  -> 1 S3 sink connector (gộp `topics`)
    debezium.py              # N dataset  -> 1 source connector (gộp table.include.list)
    postgres_publication.py  # N dataset  -> publication SQL (cùng nguồn với debezium)
    clickhouse_ddl.py        # 1 metric   -> 3 đối tượng ClickHouse (bảng + kafka + MV)
    dlq.py                   # chính sách DLQ + bản kê topic cho dlq-processor
```

Hai **hình dạng generator** khác nhau, đáng để ý:

| Hình dạng | Ví dụ | Đặc điểm |
|---|---|---|
| 1 dataset → 1 file | `es_sink.py` | Chỉ cần nhìn một contract |
| N dataset → 1 file | `s3_sink.py`, `debezium.py`, `postgres_publication.py`, `dlq.py` | Phải nhìn **toàn bộ** registry để gộp |

Hình dạng fan-in (N→1) là chỗ diệt được sprawl nguy hiểm nhất: `debezium.py` và
`postgres_publication.py` cùng đọc một nguồn, nên `table.include.list` (connector) và `FOR TABLE`
(publication) **không thể lệch nhau** ([ADR-0018](../decisions/0018-generate-debezium-and-publication.md)).

Artifact cũng có **hai loại**: `dict` → ghi JSON (connector, bản kê); `str` → ghi text nguyên văn (SQL
publication). `check` so JSON theo ngữ nghĩa, so SQL nguyên văn.

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
| **Nửa Flink của sprawl #8 còn hở** | DDL sink trong `lane1_dashboard.py` **vẫn viết tay** → vẫn có thể lệch với ClickHouse. Hết khi Flink runner sinh cả hai đầu từ cùng spec (Pha 3). |
| Chưa sinh job Spark, catalog Trino | Sprawl #10/#11 (Pha 5), #13. |
| Chưa có Deployer | `write` chỉ ghi đĩa; đẩy vào engine vẫn làm tay bằng `curl`. |
| Chưa có CI | `check` chạy tay. Sửa contract mà quên `write`, hoặc sửa tay artifact → chưa có gì chặn. |
| Contract chưa đối chiếu ngược với DB thật | Nó khớp *artifact*, chưa chắc khớp *schema Postgres*. Pha 1 còn nợ. |
