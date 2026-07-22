# ADR-0004: `REPLICA IDENTITY FULL` cho `accounts`/`transfers`, DEFAULT cho phần còn lại

- **Status:** Accepted
- **Date:** 2026-07-15 *(hồi tố)*
- **Deciders:** Phan Trường

## Bối cảnh

Mặc định (`REPLICA IDENTITY DEFAULT`), PostgreSQL chỉ ghi **khoá chính** vào WAL cho UPDATE/DELETE.
Nghĩa là message CDC có `after` đầy đủ nhưng `before` **chỉ có PK**.

Với hai bảng trong hệ thống, mất `before` là mất thông tin nghiệp vụ thật:

- `accounts.balance` đổi liên tục. Câu "số dư đổi từ bao nhiêu sang bao nhiêu" là câu hỏi audit cốt
  lõi của ngân hàng, và không trả lời được nếu không có before-image.
- `transfers.status` chạy theo state machine `pending → processing → completed/failed/cancelled`.
  Không có `before` thì không phân biệt được `processing → failed` với `pending → failed` — hai tình
  huống nghiệp vụ rất khác nhau, và là chất liệu cho CEP.

Nhưng `REPLICA IDENTITY FULL` **ghi toàn bộ row cũ vào WAL cho mỗi UPDATE/DELETE**. Trên bảng có
throughput cao, chi phí đó là thật.

## Quyết định

Bật `REPLICA IDENTITY FULL` **chọn lọc** cho hai bảng có thay đổi mang ý nghĩa nghiệp vụ; giữ DEFAULT
cho hai bảng còn lại.

```sql
ALTER TABLE accounts  REPLICA IDENTITY FULL;   -- balance cần audit
ALTER TABLE transfers REPLICA IDENTITY FULL;   -- state machine cần trạng thái trước
-- customers:    DEFAULT — dimension chậm đổi, before-image không đáng giá WAL
-- transactions: DEFAULT — append-only, không bao giờ update → before-image vô nghĩa
```

## Hệ quả

**Dễ hơn:**
- Audit được thay đổi số dư và chuyển trạng thái transfer từ chính CDC stream.
- Trả được cái giá WAL **chỉ ở nơi cần**: `transactions` là bảng nóng nhất (150–800 RPS) và nó dùng
  DEFAULT, nên phần lớn tải ghi **không** bị phạt.

**Khó hơn / phải chấp nhận:**
- WAL phình to hơn trên `accounts`/`transfers` — comment trong
  [`02_schema.sql`](../../postgres/init/02_schema.sql) ghi rõ: *"Cost: WAL phình to hơn vì ghi cả row
  cũ. Chấp nhận vì balance cần audit."*
- Cấu hình trông **thiếu nhất quán** nếu không biết lý do — chính là lý do ADR này tồn tại.
- Đây là một thuộc tính schema nữa phải nhớ khi thêm bảng. Contract metadata trong
  [lộ trình](../roadmap/BDP-metadata-driven-roadmap.md) ghi nó thành trường
  `cdc.replica_identity: full|default`.

Kiểm chứng — `02_schema.sql` kết thúc bằng một sanity check chạy ngay lúc khởi tạo:
```sql
SELECT c.relname, CASE c.relreplident
    WHEN 'd' THEN 'DEFAULT (PK only)' WHEN 'f' THEN 'FULL (all columns)' ... END
FROM pg_class c ...;
```

## Phương án đã cân nhắc

- **FULL cho tất cả 4 bảng.** Bị loại: `transactions` là append-only ở 150–800 RPS. Nó không bao giờ
  UPDATE, nên before-image **luôn** rỗng — trả giá WAL để lấy về không gì cả.
- **DEFAULT cho tất cả.** Bị loại: mất khả năng audit số dư và chuyển trạng thái transfer — chính là
  phần nghiệp vụ thú vị nhất của hệ thống.
- **`REPLICA IDENTITY USING INDEX`.** Bị loại: chỉ ghi các cột của index được chỉ định. Rẻ hơn FULL,
  nhưng vẫn phải chọn trước cột nào cần — và với audit số dư thì ta muốn **cả row** cũ.
