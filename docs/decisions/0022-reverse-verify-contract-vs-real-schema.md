# ADR-0022: Kiểm chứng ngược — contract vs schema THẬT (Postgres, ClickHouse)

- **Status:** Accepted
- **Date:** 2026-07-18
- **Deciders:** Phan Trường

## Bối cảnh

Tới giờ contract mới được chứng minh khớp với **artifact** (`cli.py check`) và với **engine sinh ra từ
chính nó** (ClickHouse ở ADR-0019, Kafka ở ADR-0020). Cả hai đều **hơi vòng**: nếu `columns` trong
contract sai, artifact sinh ra sẽ sai theo, và `check` vẫn **xanh** — vì nó chỉ so bản sinh với chính
contract sai đó.

Còn thiếu một tầng: đối chiếu contract với **nguồn sự thật ĐỘC LẬP**. Với OLTP, nguồn đó là schema
Postgres — contract được reverse-engineer *từ* nó, không phải ngược lại. Đây là nợ ẩn: một cột lệch kiểu
so với DB thật sẽ chảy âm thầm vào mọi artifact mà không tín hiệu nào.

Đây là hạng mục "kiểm chứng ngược" của Pha 1 trong [roadmap](../roadmap/BDP-metadata-driven-roadmap.md).

## Quyết định

Thêm `dataplatform/verifiers/` với hai verifier, mỗi cái hỏi một câu khác `check`:

| | Hỏi gì | Chuẩn là ai |
|---|---|---|
| `cli.py check` | Artifact có khớp metadata? | metadata |
| `verifiers/postgres_schema` | Metadata có khớp Postgres thật? | **Postgres** |
| `verifiers/clickhouse_schema` | ClickHouse đang chạy có còn khớp metadata? | metadata (bắt drift) |

### Postgres — nguồn sự thật độc lập

So từng contract CDC với `information_schema.columns`: **tên cột** (contract thừa/thiếu so với DB),
**kiểu** (logic có tương thích kiểu Postgres thật), **nullable**, **primary key**. Đây là verifier có
giá trị cao nhất vì Postgres độc lập với contract.

### ClickHouse — bắt drift, không phải "tin tức"

Quan hệ ở đây **hơi vòng**: bảng ClickHouse sinh *từ* contract, nên "khớp" là mong đợi. Giá trị của nó
là bắt **drift thủ công**: `ALTER TABLE` tay, bảng phiên bản cũ còn sót, hoặc `write` chưa `apply`. Nó
**tái dùng** `clickhouse_ddl._ch_type` để tính kiểu kỳ vọng — không viết lại ánh xạ kiểu, nếu không lại
đẻ ra hai nguồn tri thức tự mâu thuẫn.

### Không cầm credential

Cả hai chạy client **bên trong** container bằng chính env của nó
(`sh -c 'psql -U "$POSTGRES_USER" ...'`, `clickhouse-client`). Verifier không bao giờ đọc hay truyền mật
khẩu — cùng nguyên tắc với deployer ([ADR-0021](0021-connector-deployer-idempotent.md)).

## Kiểm chứng — chứng minh verifier THẬT SỰ bắt lệch

Một verifier luôn báo "khớp" thì vô dụng. Nên kiểm hai chiều:

- **Postgres:** tiêm 3 lỗi vào `transactions.yaml` (kiểu `amount` → `decimal(10,2)`, PK sai, cột ma
  `ghost_col`). Verifier bắt **đủ 3**, đúng loại. `git checkout` khôi phục → khớp lại.
- **ClickHouse:** `ALTER TABLE metrics.timeseries ADD COLUMN drift_col` thật. Verifier bắt ngay cột
  drift. `DROP COLUMN` → sạch lại.

## Kết quả — nợ ẩn **không tồn tại**

Chạy trên stack thật: **4/4 contract CDC khớp Postgres, 4/4 bảng metric khớp ClickHouse, 0 lệch.**

Đây là tin tốt và là điểm chính của ADR: sự trung thực của contract với nguồn sự thật nay được **chứng
minh**, không còn là **giả định**. Trước Pha 5 (lake) — nơi sẽ có thêm nhiều dataset suy từ các bảng
này — móng đã được kiểm, không xây trên đất chưa dò.

## Hệ quả

**Dễ hơn / chắc hơn:**
- Có công cụ đứng-một-lệnh để trả lời "contract có còn đúng nguồn không" — chạy khi nghi ngờ, hoặc định kỳ.
- ClickHouse verifier bắt drift thủ công mà `check` không thấy (vì `check` so đĩa, không so engine chạy).

**Khó hơn / phải chấp nhận:**
- Verifier **cần stack chạy** (khác `check` thuần tĩnh), nên KHÔNG vào CI cơ bản được. Nó thuộc CI có
  **stack ephemeral** (Pha 7), hoặc chạy tay khi vận hành.
- **Còn thiếu nguồn thứ ba: Avro trong Schema Registry** — cái Debezium THẬT SỰ phát ra trên Kafka
  (kiểm `encoded_as: string` từ `decimal.handling.mode`). Đó là nguồn độc lập nữa, chưa làm ở ADR này.

## Phương án đã cân nhắc

- **Dùng `psycopg`/driver + kết nối qua cổng host.** Bị loại: thêm phụ thuộc và phải cầm credential.
  `docker exec` client-trong-container tránh cả hai.
- **Viết lại ánh xạ kiểu ClickHouse trong verifier.** Bị loại: tạo nguồn tri thức kiểu thứ hai, tự mâu
  thuẫn — đúng thứ sprawl đang diệt. Tái dùng `_ch_type` của generator.
- **Đối chiếu ClickHouse bằng `SHOW CREATE` như ADR-0019.** Bị loại cho verifier đứng-lâu: `system.columns`
  gọn hơn để so từng cột; `SHOW CREATE` hợp cho oracle một-lần lúc cắt chuyển.
- **Bỏ ClickHouse vì vòng.** Bị loại: tuy vòng, nó vẫn bắt drift thủ công — một lớp bảo vệ thật, chi phí
  thấp nhờ tái dùng.
