-- Database đã tự tạo qua env CLICKHOUSE_DB=metrics

-- timeseries: tumbling 1-min, breakdown theo transaction_type
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

-- kpi: cumulative 5-min, 6 KPI tổng

CREATE TABLE IF NOT EXISTS metrics.kpi (
    window_start    DateTime64(3),
    window_end      DateTime64(3),
    total_count     UInt64,
    total_value     Decimal(19, 4),
    success_count   UInt64,
    failed_count    UInt64,
    success_rate    Decimal(5, 2),
    active_users    UInt64,
    inserted_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, window_end)
TTL toDateTime(window_start) + INTERVAL 90 DAY;

-- 3. breakdown: cumulative 5-min, theo transaction_type
CREATE TABLE IF NOT EXISTS metrics.breakdown (
    window_start    DateTime64(3),
    window_end      DateTime64(3),
    tx_type         LowCardinality(String),
    tx_count        UInt64,
    total_value     Decimal(19, 4),
    success_count   UInt64,
    failed_count    UInt64,
    inserted_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, window_end, tx_type)
TTL toDateTime(window_start) + INTERVAL 30 DAY;

-- 4. topn: cumulative 5-min, top 10 accounts

CREATE TABLE IF NOT EXISTS metrics.topn (
    window_start    DateTime64(3),
    window_end      DateTime64(3),
    rank_num        UInt8,
    account_id      UInt64,
    tx_count        UInt64,
    total_value     Decimal(19, 4),
    inserted_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, window_end, rank_num)
TTL toDateTime(window_start) + INTERVAL 30 DAY;