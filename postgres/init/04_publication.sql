-- FILE SINH TỰ ĐỘNG — đừng sửa tay. Sinh lại: python -m dataplatform.cli write

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
