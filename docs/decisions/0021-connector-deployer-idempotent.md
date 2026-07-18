# ADR-0021: Deployer connector idempotent — biến metadata thành load-bearing

- **Status:** Accepted
- **Date:** 2026-07-18
- **Deciders:** Phan Trường

## Bối cảnh

Control plane sinh 7 config connector từ registry và `check` xanh 13/13. Nhìn thì như xong, nhưng có
một cái bẫy: **các file đó chưa gánh gì cả**. Thứ THẬT SỰ cấu hình Kafka Connect vẫn là bàn tay người
gõ `curl -X POST` từ [`docs/guide/cdc-and-connectors.md`](../guide/cdc-and-connectors.md). Nghĩa là
`metadata/` mới là một **bản sao rất chuẩn nhưng để trang trí** — vẫn phải giữ đồng bộ với hệ thống thật
bằng tay, đúng cái bệnh metadata-driven muốn chữa.

Giá trị "một nơi để sửa" chưa hiện thực hoá tới khi **deploy cũng đọc từ metadata**. Đây là hạng mục
Deployer của Pha 2 trong [roadmap](../roadmap/BDP-metadata-driven-roadmap.md).

## Quyết định

Viết `dataplatform/deployers/connectors.py`: đọc desired state THẲNG từ generator, áp lên Connect REST
**idempotent**, có `plan` (chỉ xem) và `apply` — đúng mô hình plan/apply như Terraform.

### `PUT /connectors/{name}/config`, không `POST /connectors`

- `POST` tạo mới; gọi lần hai trên connector đã tồn tại → **409 Conflict**.
- `PUT .../config` là "đặt config này làm hiện trạng": chưa có thì tạo, có rồi thì cập nhật. **Idempotent**
  → chạy lại bao nhiêu lần cũng an toàn.

### Đọc thẳng generator, không đọc file trên đĩa

Desired state suy từ `es_sink.targets() + s3_sink.targets() + debezium.targets()`, không phải đọc JSON
trên đĩa. Nhờ vậy **không thể áp bản cũ**: deployer luôn áp đúng cái contract mới nhất mô tả. `check` lo
việc "đĩa khớp metadata"; deployer lo việc "hệ thống khớp metadata" — hai tầng khác nhau.

### Placeholder `${env:...}` giữ nguyên — deployer không bao giờ chạm secret

Config có `"database.password": "${env:REPLICATION_PASSWORD}"`. Deployer gửi NGUYÊN chuỗi đó; Connect tự
resolve bằng `EnvVarConfigProvider` bên trong container. Deployer không đọc, không thấy, không truyền
secret — một tính chất an toàn quan trọng, không phải tình cờ.

### `plan` là mặc định

Giống `check`: mặc định KHÔNG ghi. `plan` GET config hiện tại, so với desired, in `CREATE`/`UPDATE`/
`UNCHANGED`. Chỉ `apply` mới PUT. So sánh chỉ xét khoá TRONG desired (Connect tự thêm `name` vào config
lưu — không tính là khác biệt).

## Kiểm chứng — deploy thật rồi chứng minh idempotent

1. `apply` lần 1: 7 connector `CREATE`, cả 7 vào **RUNNING** (có bước đợi status, vì PUT trả 200 ngay khi
   Connect NHẬN config chứ chưa chắc task đã chạy — snapshot Debezium mất vài giây).
2. `apply` lần 2: **"Mọi connector đã khớp desired state — không có gì để áp."** Idempotency chứng minh
   bằng hành vi, không phải bằng niềm tin.
3. Sau khi **recreate container kafka** (lúc tắt auto-create), cả 7 connector tự reconnect và vẫn
   RUNNING; dữ liệu snapshot (accounts=200, customers=100) chảy trọn Postgres → topic → ES.

## Hệ quả

**Dễ hơn:**
- Thêm/sửa connector = sửa contract → `python -m dataplatform.deployers.connectors apply`. Hết `curl`
  thủ công. Metadata giờ **load-bearing**, không còn trang trí.
- `plan` cho thấy chính xác sẽ đổi gì trước khi đổi — nền cho CI "plan → apply" (Pha 7).
- Không phụ thuộc thư viện ngoài (dùng `urllib` chuẩn), không chạm secret.

**Khó hơn / phải chấp nhận:**
- Deployer chạy từ máy có mạng tới Connect (`localhost:8083`, override bằng `CONNECT_URL`). Trong CI/CD
  sau này phải chạy trong mạng compose (`http://kafka-connect:8083`).
- Chưa xử lý **xoá**: connector có trong Connect nhưng không còn trong metadata thì deployer hiện KHÔNG
  gỡ. Cần thêm khi muốn "metadata là toàn quyền" (reconcile hai chiều).
- So sánh chỉ một chiều (desired ⊆ current). Đủ để biết "cần áp không", chưa phát hiện khoá thừa Connect
  tự thêm ngoài `name`.

## Phương án đã cân nhắc

- **Giữ `curl` thủ công.** Bị loại: đó chính là bước tay mà metadata-driven phải xoá. Để nguyên thì
  generator mãi chỉ là tài liệu.
- **POST + xử lý 409.** Bị loại: PUT idempotent sẵn, không cần bắt lỗi trùng.
- **Đọc JSON trên đĩa để áp.** Bị loại: mở khả năng áp bản cũ (đĩa lệch metadata mà chưa chạy generator).
  Đọc thẳng generator là nguồn sự thật duy nhất.
- **Resolve secret rồi PUT giá trị thật.** Bị loại dứt khoát: deployer sẽ phải cầm secret. Để Connect tự
  resolve `${env:...}` an toàn hơn hẳn.
- **Gỡ connector thừa (reconcile hai chiều) ngay.** Bị hoãn: hiện chưa có connector "mồ côi"; thêm khi cần.
