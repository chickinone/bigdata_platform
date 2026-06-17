
-- Publication = declarative API của Postgres để publish bảng cho
-- logical replication. Debezium sẽ subscribe vào publication này.

-- Tạo publication chỉ include 4 bảng cần CDC
-- Note: Không dùng "FOR ALL TABLES" vì:
--   1. Rủi ro bảo mật - bảng nhạy cảm mới tạo tự động bị publish
--   2. Khó audit "Debezium đang đọc bảng nào"
--   3. Explicit is better than implicit
CREATE PUBLICATION dbz_publication
    FOR TABLE
        public.customers,
        public.accounts,
        public.transactions,
        public.transfers
    WITH (publish = 'insert, update, delete');
    -- publish param mặc định đã là insert,update,delete,truncate.
    -- Set tường minh để rõ ràng. Có thể bỏ truncate nếu source không bao giờ TRUNCATE.
-- Explicit GRANT cho replicator (defensive)
-- Mặc dù 01_users.sql đã có ALTER DEFAULT PRIVILEGES, ta vẫn grant
-- tường minh ở đây cho an toàn (nếu ai đó modify 01_users.sql).
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO replicator;

-- Verify
SELECT
    pubname,
    puballtables AS for_all_tables,
    pubinsert    AS pub_insert,
    pubupdate    AS pub_update,
    pubdelete    AS pub_delete,
    pubtruncate  AS pub_truncate
FROM pg_publication
WHERE pubname = 'dbz_publication';

SELECT pubname, schemaname, tablename
FROM pg_publication_tables
WHERE pubname = 'dbz_publication'
ORDER BY tablename;
