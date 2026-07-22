-- =====================================================================
-- FILE SINH TỰ ĐỘNG — đừng sửa tay.
--   Nguồn:    metadata/datasets/metrics/*.yaml
--   Sinh lại: python -m dataplatform.cli write
--
-- Bảng đích cho mỗi metric (nơi lưu thật, Grafana đọc).
-- Cột sinh từ `columns` của contract, nên luôn khớp bảng Kafka + MV
-- ở 02_kafka_consumers.sql (diệt sprawl #8/#9).
-- =====================================================================

-- bank.metric.breakdown
CREATE TABLE IF NOT EXISTS metrics.breakdown (
    window_start   DateTime64(3),
    window_end     DateTime64(3),
    tx_type        LowCardinality(String),
    tx_count       UInt64,
    total_value    Decimal(19, 4),
    success_count  UInt64,
    failed_count   UInt64,
    inserted_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, window_end, tx_type)
TTL toDateTime(window_start) + INTERVAL 30 DAY;

-- bank.metric.kpi
CREATE TABLE IF NOT EXISTS metrics.kpi (
    window_start   DateTime64(3),
    window_end     DateTime64(3),
    total_count    UInt64,
    total_value    Decimal(19, 4),
    success_count  UInt64,
    failed_count   UInt64,
    success_rate   Decimal(5, 2),
    active_users   UInt64,
    inserted_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, window_end)
TTL toDateTime(window_start) + INTERVAL 90 DAY;

-- bank.metric.timeseries
CREATE TABLE IF NOT EXISTS metrics.timeseries (
    window_start  DateTime64(3),
    window_end    DateTime64(3),
    tx_type       LowCardinality(String),
    tx_count      UInt64,
    total_amount  Decimal(19, 4),
    inserted_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, tx_type)
TTL toDateTime(window_start) + INTERVAL 30 DAY;

-- bank.metric.topn
CREATE TABLE IF NOT EXISTS metrics.topn (
    window_start  DateTime64(3),
    window_end    DateTime64(3),
    rank_num      UInt8,
    account_id    UInt64,
    tx_count      UInt64,
    total_value   Decimal(19, 4),
    inserted_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, window_end, rank_num)
TTL toDateTime(window_start) + INTERVAL 30 DAY;
