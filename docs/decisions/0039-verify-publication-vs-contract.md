# ADR-0039: Verifier publication + replica identity — đóng lỗ hổng runtime của mối nối CDC

- **Status:** Accepted — chạy trên Postgres thật, xanh ở chiều dương và bắt đúng ở hai chiều âm
- **Date:** 2026-07-29
- **Deciders:** Phan Trường

## Bối cảnh

[ADR-0018](0018-generate-debezium-and-publication.md) đóng mối nối publication ↔ `table.include.list` bằng
cách cho `postgres_publication.py` và `debezium.py` cùng gọi `cdc_datasets()`. Hai **artifact** từ đó không
thể lệch nhau, và `cli check` gác thêm một lớp trong CI.

Nhưng artifact đúng không có nghĩa **Postgres đang chạy** đúng. `postgres/init/04_publication.sql` là init
script — chỉ chạy khi DB mới tạo. Thêm một bảng CDC vào contract trên DB đang sống thì: file trên đĩa đúng,
`cli check` xanh 19/19, connector `RUNNING` — mà publication thật vẫn thiếu bảng.

Lệch này nguy hiểm hơn mọi lệch khác trong hệ thống vì nó **giả vờ hoạt động**:

- snapshot ban đầu vẫn chạy (Debezium `SELECT` thẳng bảng, không qua publication) → bảng **có** dữ liệu cũ
  trong Kafka;
- streaming thì im lặng (pgoutput chỉ giải mã bảng có trong publication) → bảng **không bao giờ** có bản ghi
  mới;
- không exception, không log lỗi, không đổi trạng thái connector.

Nhìn vào hệ thống thấy khoẻ và thấy có dữ liệu — chỉ là dữ liệu đứng im mãi mãi. Đây là lớp lỗi không phát
hiện được bằng cách quan sát trạng thái; cách duy nhất là hỏi thẳng Postgres.

`source.replica_identity` cùng một hình dạng: khai trong contract, chưa nơi nào đối chiếu, và hỏng im lặng
theo cách y hệt — khai `full` mà DB là `default` thì UPDATE/DELETE mất before-image ([ADR-0004](0004-replica-identity-full-for-mutable-tables.md)).

## Quyết định

Thêm `verifiers/postgres_publication.py` — đối chiếu **cấu hình CDC phía Postgres** với contract. Kiểm 4 thứ:

1. **Publication tồn tại** — không có thì Debezium FAILED ngay lúc khởi động
   (`publication.autocreate.mode=disabled`). Phân biệt rõ với ca "có nhưng thiếu bảng", vì một cái ồn ào
   một cái im lặng.
2. **Tập bảng** — `THIẾU` (contract có, Postgres không) là error, kèm sẵn câu `ALTER PUBLICATION ... ADD
   TABLE` để copy chạy. `THỪA` là warning (Debezium lọc lại bằng `table.include.list`, chỉ phí WAL).
3. **Phép được publish** — generator khai `publish = 'insert, update, delete'`; thiếu `delete` thì bản ghi
   xoá không tới ES/lake, cũng im lặng.
4. **Replica identity** — so `pg_class.relreplident` với `source.replica_identity`. Contract không khai thì
   bỏ qua ("không khai" khác "khai default").

Verifier **chỉ đọc**, không tự `ALTER`. In lệnh sửa ra cho người chạy quyết định.

### Vì sao verifier chứ không phải migration runner

Migration runner cho Postgres (kiểu [ADR-0032](0032-versioned-migration-clickhouse.md) đã làm cho ClickHouse)
sẽ diệt luôn cả lớp vấn đề "init-once trên DB sống", không riêng publication. Nhưng Postgres ở đây là **hệ
thống nguồn của ứng dụng**, không thuộc quyền sở hữu của data platform — cho control plane tự ý `ALTER` là
mở rộng quyền vượt ranh giới đã chốt ở [ADR-0022](0022-reverse-verify-contract-vs-real-schema.md). Verifier
rẻ hơn nhiều, và giải quyết đúng phần đắt nhất của vấn đề: **biến lỗi im lặng thành lỗi ồn ào**. Ai sửa và
sửa lúc nào vẫn là quyết định của con người.

Ranh giới giữ nguyên như toàn bộ `verifiers/`: verifier đọc, deployer ghi. Trộn hai vai vào một công cụ là
cách nhanh nhất để có thứ tự ý ALTER mà không ai kiểm soát được.

### Tái dùng, không viết lại

`_psql` lấy từ `verifiers/postgres_schema.py`; `PUBLICATION_NAME` + `cdc_datasets()` lấy từ
`generators/debezium.py`. Nên verifier và generator không thể có hai định nghĩa "bảng CDC nào" — cùng lý do
`clickhouse_schema` tái dùng `clickhouse_ddl._ch_type`.

## Kiểm chứng (đo thật)

Trên `bigdata-source-postgres` đang chạy, 4 dataset CDC:

- **Chiều dương:** 4/4 bảng `[KHỚP]`, replica identity khớp, 0 lệch, exit 0.
- **Chiều âm 1** — `ALTER PUBLICATION dbz_publication DROP TABLE public.transfers`: báo `[THIẾU]
  public.transfers` kèm câu ALTER khôi phục, exit 1.
- **Chiều âm 2** — `ALTER TABLE public.transfers REPLICA IDENTITY DEFAULT` (contract khai `full`): báo lệch
  `contract='full' vs DB='default'` kèm hệ quả before-image, exit 1.
- Khôi phục cả hai → xanh trở lại.

Trong cả hai ca âm, `cli check` vẫn **19/19 xanh** — đúng thứ verifier sinh ra để bắt.

## Hệ quả

**Dễ hơn:** lệch cấu hình CDC phía Postgres phát hiện trong 2 giây thay vì vài tuần. Bước "thêm bảng CDC"
trong runbook có điểm dừng kiểm tra thay vì trông vào trí nhớ.

**Khó hơn / phải chấp nhận:**

- Cần Postgres sống → thuộc nhóm verifier runtime (`postgres_schema`, `clickhouse_schema`, `quality`),
  không vào CI tĩnh.
- Vẫn phải tự gõ `ALTER PUBLICATION` khi thêm bảng trên DB sống. Verifier bắt được việc quên, không làm hộ.
- Hai nhánh chưa chạy qua: publication không tồn tại, và `THỪA` bảng. Đường code thẳng nhưng chưa chứng minh.

## Phương án đã cân nhắc

- **Migration runner cho Postgres.** Hoãn: triệt để hơn nhưng vượt ranh giới sở hữu (xem trên). Cân nhắc
  lại nếu số lần "quên ALTER" đủ nhiều để trả giá.
- **`publication.autocreate.mode=filtered`** để Debezium tự đồng bộ publication. Loại: publication khi đó
  có hai chủ (SQL sinh + Debezium), mất khả năng audit ai đổi gì — đúng thứ [ADR-0018](0018-generate-debezium-and-publication.md)
  cố tình tránh.
- **Cảnh báo dựa trên lag/throughput theo topic.** Loại (giờ): phát hiện gián tiếp và chậm, không phân biệt
  được "bảng im vì thiếu publication" với "bảng im vì không có giao dịch".
- **Gộp vào `postgres_schema.py`.** Loại: file đó trả lời "contract có đúng schema bảng không"; file này trả
  lời "Postgres có được cấu hình đúng như contract nói không". Hai câu hỏi khác nhau, hai exit code riêng.
