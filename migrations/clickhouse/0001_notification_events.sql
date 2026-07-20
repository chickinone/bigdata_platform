-- Migration 0001 — tạo metrics.notification_events.
--
-- Vì sao là MIGRATION chứ không nằm trong init sinh-từ-contract: đây là bảng LOG
-- hạ tầng (fraud-notifier ghi vào), không phải một metric contract. `fraud_notifier.py`
-- INSERT vào bảng này (rule_type, account_id, severity, action, description) nhưng
-- trước nay KHÔNG init nào tạo nó -> mọi lần ghi đều fail âm thầm (nợ 4b ở
-- BDP-current-state). Migration này đóng nợ đó + minh hoạ tầng versioned.
--
-- BẤT BIẾN: đã áp thì đừng sửa file này; thay đổi sau = tạo migration mới.

CREATE TABLE IF NOT EXISTS metrics.notification_events (
    detected_at   DateTime64(3) DEFAULT now64(3),
    rule_type     LowCardinality(String),
    account_id    String,
    severity      LowCardinality(String),
    action        LowCardinality(String),
    description   String
)
ENGINE = MergeTree
ORDER BY (detected_at, rule_type)
TTL toDateTime(detected_at) + INTERVAL 90 DAY;
