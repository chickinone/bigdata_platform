# ADR-0002: CDC bằng Debezium + `pgoutput`, publication khai báo tường minh

- **Status:** Accepted
- **Date:** 2026-07-15 *(hồi tố — ghi lại quyết định đã có trong code)*
- **Deciders:** Phan Trường

## Bối cảnh

Nền tảng cần đưa thay đổi từ PostgreSQL sang Kafka gần thời gian thực, mà **không** làm nguồn OLTP
phải chịu thêm tải truy vấn.

## Quyết định

Dùng **Debezium PostgreSQL Connector** đọc WAL qua plugin logical decoding **`pgoutput`**, đọc từ một
publication **khai báo tường minh 4 bảng**, với `publication.autocreate.mode = disabled`.

```json
"plugin.name": "pgoutput",
"slot.name": "debezium_slot",
"publication.name": "dbz_publication",
"publication.autocreate.mode": "disabled",
"table.include.list": "public.customers,public.accounts,public.transactions,public.transfers"
```

```sql
CREATE PUBLICATION dbz_publication
    FOR TABLE public.customers, public.accounts, public.transactions, public.transfers
    WITH (publish = 'insert, update, delete');
```

## Hệ quả

**Dễ hơn:**
- Không cần cài extension — `pgoutput` có sẵn trong PostgreSQL 10+ (khác `wal2json`/`decoderbufs`).
- Nguồn OLTP chỉ tốn thêm một replication slot, không có truy vấn polling nào.
- Publication tường minh **audit được**: `SELECT * FROM pg_publication_tables` trả lời chính xác câu
  "Debezium đang đọc bảng nào".
- Bảng nhạy cảm tạo mới **không** tự động bị publish — điểm này `FOR ALL TABLES` không đảm bảo được.

**Khó hơn / phải chấp nhận:**
- **Danh sách bảng bị khai báo hai nơi** — publication SQL *và* `table.include.list`. Lệch nhau thì
  bảng thiếu sẽ **im lặng không có CDC**. Đây là metadata sprawl #2 và #3 ở
  [`../architecture/BDP-current-state.md`](../architecture/BDP-current-state.md) §3, và là lý do
  [Pha 2 của lộ trình](../roadmap/BDP-metadata-driven-roadmap.md) sinh **cả hai** từ một contract.
- Thêm bảng vào CDC = 2 thao tác thủ công (`ALTER PUBLICATION` + sửa connector).
- **Replication slot mồ côi làm đầy đĩa.** Xoá connector không xoá slot; slot không consumer giữ WAL
  vô hạn. Giảm nhẹ bằng `heartbeat.interval.ms=10000` để `restart_lsn` vẫn tiến khi bảng im lặng.
- Debezium Postgres **luôn 1 task** (một slot đọc tuần tự) — không scale ngang được ở chặng CDC.

Yêu cầu kèm theo: Postgres phải chạy `wal_level=logical`, `max_replication_slots=5`,
`max_wal_senders=10` — đã đặt trong `docker-compose.yml`.

## Phương án đã cân nhắc

- **`FOR ALL TABLES`.** Bị loại vì 3 lý do đã ghi ngay trong
  [`04_publication.sql`](../../postgres/init/04_publication.sql): (1) rủi ro bảo mật — bảng nhạy cảm
  mới tạo tự động bị publish; (2) khó audit "Debezium đang đọc bảng nào"; (3) tường minh tốt hơn ngầm
  định.
- **`wal2json` / `decoderbufs`.** Bị loại: phải cài extension vào image Postgres. `pgoutput` có sẵn và
  được Debezium hỗ trợ tốt nhất.
- **Polling theo `updated_at`.** Bị loại: bỏ sót DELETE, bỏ sót các thay đổi trung gian giữa hai lần
  poll, và đổ tải truy vấn lên chính nguồn OLTP.
- **Trigger ghi bảng outbox.** Bị loại: xâm lấn schema nguồn và thêm chi phí ghi cho mọi giao dịch.
