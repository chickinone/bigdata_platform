# ADR-0022: Kiểm chứng ngược — contract vs schema thật (Postgres, ClickHouse)

- **Status:** Accepted
- **Date:** 2026-07-18
- **Deciders:** Phan Trường

## Bối cảnh

Tới giờ contract mới được chứng minh khớp với **artifact** (`cli.py check`) và với **engine sinh ra từ
chính nó** (ClickHouse ở ADR-0019, Kafka ở ADR-0020). Cả hai đều **hơi vòng**: nếu `columns` trong
contract sai, artifact sinh ra sẽ sai theo, và `check` vẫn **xanh** — vì nó chỉ so bản sinh với chính
contract sai đó.

Còn thiếu một tầng: đối chiếu contract với **nguồn sự thật độc lập**. Với OLTP, nguồn đó là schema
Postgres — contract được reverse-engineer *từ* nó, không phải ngược lại. Đây là nợ ẩn: một cột lệch kiểu
so với DB thật sẽ chảy âm thầm vào mọi artifact mà không tín hiệu nào.

Đây là hạng mục "kiểm chứng ngược" của Pha 1 trong [roadmap](../roadmap/BDP-metadata-driven-roadmap.md).

## Quyết định

Thêm `dataplatform/verifiers/` với hai verifier, mỗi cái hỏi một câu khác `check`:

| | Hỏi gì | Chuẩn là ai |
|---|---|---|
| `cli.py check` | Artifact có khớp metadata? | metadata |
| `verifiers/postgres_schema` | Metadata có khớp Postgres thật? | **Postgres** (bảng nguồn) |
| `verifiers/avro_schema` | Metadata có khớp cái Debezium phát trên dây? | **Avro/Schema Registry** (trên dây) |
| `verifiers/clickhouse_schema` | ClickHouse đang chạy có còn khớp metadata? | metadata (bắt drift) |

Ba nguồn sự thật độc lập nhìn dữ liệu ở ba điểm khác nhau: bảng nguồn (Postgres), trên dây (Avro), bảng
đích (ClickHouse). Mỗi cái bắt lỗi hai cái kia không thấy.

### Postgres — nguồn sự thật độc lập

So từng contract CDC với `information_schema.columns`: **tên cột** (contract thừa/thiếu so với DB),
**kiểu** (logic có tương thích kiểu Postgres thật), **nullable**, **primary key**. Đây là verifier có
giá trị cao nhất vì Postgres độc lập với contract.

### Avro — cái Debezium thật sự phát trên dây

Nguồn độc lập thấy thứ Postgres verifier không thấy: **mã hoá trên Kafka**. Cột `balance` là
`numeric(19,4)` trong Postgres nhưng trên dây là **`string`** — do `decimal.handling.mode=string`
([ADR-0003](0003-avro-with-schema-registry.md)). Contract phải khai `encoded_as: string`, và generator
dựa vào đó để chèn `CAST`. Verifier so kiểu Avro thật trong Schema Registry (record `after`/`before` của
envelope Debezium) với `type` + `encoded_as` của contract — khẳng định quan trọng nhất: cột khai
`encoded_as: string` thì trên dây phải là `string`, và cột decimal mà dây là string nhưng contract quên
`encoded_as` sẽ bị cảnh báo (generator sẽ thiếu `CAST`). Dataset bảng rỗng chưa produce → chưa có subject
→ **bỏ qua** (không phải lỗi).

### ClickHouse — bắt drift, không phải "tin tức"

Quan hệ ở đây **hơi vòng**: bảng ClickHouse sinh *từ* contract, nên "khớp" là mong đợi. Giá trị của nó
là bắt **drift thủ công**: `ALTER TABLE` tay, bảng phiên bản cũ còn sót, hoặc `write` chưa `apply`. Nó
**tái dùng** `clickhouse_ddl._ch_type` để tính kiểu kỳ vọng — không viết lại ánh xạ kiểu, nếu không lại
đẻ ra hai nguồn tri thức tự mâu thuẫn.

### Không cầm credential

Cả hai chạy client **bên trong** container bằng chính env của nó
(`sh -c 'psql -U "$POSTGRES_USER" ...'`, `clickhouse-client`). Verifier không bao giờ đọc hay truyền mật
khẩu — cùng nguyên tắc với deployer ([ADR-0021](0021-connector-deployer-idempotent.md)).

## Kiểm chứng — chứng minh verifier thật sự bắt lệch

Một verifier luôn báo "khớp" thì vô dụng. Nên kiểm hai chiều:

- **Postgres:** tiêm 3 lỗi vào `transactions.yaml` (kiểu `amount` → `decimal(10,2)`, PK sai, cột ma
  `ghost_col`). Verifier bắt **đủ 3**, đúng loại. `git checkout` khôi phục → khớp lại.
- **ClickHouse:** `ALTER TABLE metrics.timeseries ADD COLUMN drift_col` thật. Verifier bắt ngay cột
  drift. `DROP COLUMN` → sạch lại.
- **Avro:** bỏ `encoded_as: string` khỏi `accounts.balance` và đổi `account_number` sang `long`. Verifier
  bắt **kiểu dây lệch** (`account_number` contract=long vs dây=string) và **cảnh báo** balance là string
  mà contract thiếu `encoded_as`. `git checkout` → khớp lại.

## Kết quả — nợ ẩn **không tồn tại**

Chạy trên stack thật: **4/4 contract CDC khớp Postgres, 4/4 bảng metric khớp ClickHouse, phần Avro kiểm
được (accounts, customers) khớp — 0 lệch.** (transactions/transfers rỗng ở nguồn nên chưa có schema Avro,
verifier bỏ qua đúng cách; kiểm được nốt khi có data.)

Đây là tin tốt và là điểm chính của ADR: sự trung thực của contract với nguồn sự thật nay được **chứng
minh**, không còn là **giả định**. Trước Pha 5 (lake) — nơi sẽ có thêm nhiều dataset suy từ các bảng
này — móng đã được kiểm, không xây trên đất chưa dò.

## Hệ quả

**Dễ hơn / chắc hơn:**
- Có công cụ đứng-một-lệnh để trả lời "contract có còn đúng nguồn không" — chạy khi nghi ngờ, hoặc định kỳ.
- ClickHouse verifier bắt drift thủ công mà `check` không thấy (vì `check` so đĩa, không so engine chạy).

**Khó hơn / phải chấp nhận:**
- Verifier **cần stack chạy** (khác `check` thuần tĩnh), nên không vào CI cơ bản được. Nó thuộc CI có
  **stack ephemeral** (Pha 7), hoặc chạy tay khi vận hành.
- Verifier Avro chỉ kiểm được dataset **đã produce ít nhất một message** (subject mới tồn tại). Bảng rỗng
  bị bỏ qua cho tới khi có data — giới hạn cố hữu, không phải thiếu sót.

## Phương án đã cân nhắc

- **Dùng `psycopg`/driver + kết nối qua cổng host.** Bị loại: thêm phụ thuộc và phải cầm credential.
  `docker exec` client-trong-container tránh cả hai.
- **Viết lại ánh xạ kiểu ClickHouse trong verifier.** Bị loại: tạo nguồn tri thức kiểu thứ hai, tự mâu
  thuẫn — đúng thứ sprawl đang diệt. Tái dùng `_ch_type` của generator.
- **Đối chiếu ClickHouse bằng `SHOW CREATE` như ADR-0019.** Bị loại cho verifier đứng-lâu: `system.columns`
  gọn hơn để so từng cột; `SHOW CREATE` hợp cho oracle một-lần lúc cắt chuyển.
- **Bỏ ClickHouse vì vòng.** Bị loại: tuy vòng, nó vẫn bắt drift thủ công — một lớp bảo vệ thật, chi phí
  thấp nhờ tái dùng.
