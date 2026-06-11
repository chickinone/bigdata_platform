
-- Postgres không có ON UPDATE CURRENT_TIMESTAMP như MySQL.
-- Phải viết trigger function rồi attach vào từng bảng.
-- Generic trigger function: dùng chung cho mọi bảng có cột updated_at
-- NEW = pseudo-record chứa values mới của row đang UPDATE
-- BEFORE UPDATE: chạy trước khi UPDATE thực sự ghi, có thể sửa NEW
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

-- Attach vào 4 bảng
-- Lưu ý: transactions có updated_at không? KHÔNG.
-- transactions là append-only, không UPDATE → không cần updated_at trigger.

CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_accounts_updated_at
    BEFORE UPDATE ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_transfers_updated_at
    BEFORE UPDATE ON transfers
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();

-- ---------------------------------------------------------------------
-- Verify trigger đã tạo
-- ---------------------------------------------------------------------
SELECT
    event_object_table AS table_name,
    trigger_name,
    action_timing,
    event_manipulation
FROM information_schema.triggers
WHERE trigger_schema = 'public'
ORDER BY event_object_table, trigger_name;
