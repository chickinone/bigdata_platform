-- =====================================================================
-- FILE SINH TỰ ĐỘNG — đừng sửa tay.
--   Nguồn:    metadata/datasets/*.yaml (mọi dataset có source.type=cdc_debezium)
--   Sinh lại: python -m dataplatform.cli write
--
-- Publication = declarative API của Postgres để publish bảng cho logical
-- replication; Debezium subscribe vào đây. Danh sách bảng dưới đây được gộp từ
-- registry, nên luôn khớp table.include.list của connector (diệt sprawl #2/#3).
--
-- Không dùng "FOR ALL TABLES" vì:
--   1. Rủi ro bảo mật — bảng nhạy cảm mới tạo tự động bị publish.
--   2. Khó audit "Debezium đang đọc bảng nào".
--   3. Explicit is better than implicit.
-- =====================================================================

CREATE PUBLICATION dbz_publication
    FOR TABLE
        public.accounts,
        public.customers,
        public.transactions,
        public.transfers
    WITH (publish = 'insert, update, delete');

-- GRANT tường minh cho replicator (defensive — dù 01_users.sql đã cấp).
GRANT SELECT ON ALL TABLES    IN SCHEMA public TO replicator;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO replicator;
