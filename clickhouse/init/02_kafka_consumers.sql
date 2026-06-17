-- TIMESERIES — Kafka buffer + Materialized View
CREATE TABLE IF NOT EXISTS metrics.timeseries_kafka (
    window_start  DateTime64(3),
    window_end    DateTime64(3),
    tx_type       String,
    tx_count      UInt64,
    total_amount  Decimal(19, 4)
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'metrics.timeseries',
    kafka_group_name = 'clickhouse-timeseries',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_max_block_size = 1048576;

CREATE MATERIALIZED VIEW IF NOT EXISTS metrics.timeseries_mv
TO metrics.timeseries AS
SELECT
    window_start, window_end, tx_type, tx_count, total_amount
FROM metrics.timeseries_kafka;

-- KPI
CREATE TABLE IF NOT EXISTS metrics.kpi_kafka (
    window_start    DateTime64(3),
    window_end      DateTime64(3),
    total_count     UInt64,
    total_value     Decimal(19, 4),
    success_count   UInt64,
    failed_count    UInt64,
    success_rate    Decimal(5, 2),
    active_users    UInt64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'metrics.kpi',
    kafka_group_name = 'clickhouse-kpi',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS metrics.kpi_mv
TO metrics.kpi AS
SELECT
    window_start, window_end, total_count, total_value,
    success_count, failed_count, success_rate, active_users
FROM metrics.kpi_kafka;

-- BREAKDOWN
CREATE TABLE IF NOT EXISTS metrics.breakdown_kafka (
    window_start    DateTime64(3),
    window_end      DateTime64(3),
    tx_type         String,
    tx_count        UInt64,
    total_value     Decimal(19, 4),
    success_count   UInt64,
    failed_count    UInt64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'metrics.breakdown',
    kafka_group_name = 'clickhouse-breakdown',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS metrics.breakdown_mv
TO metrics.breakdown AS
SELECT
    window_start, window_end, tx_type, tx_count, total_value,
    success_count, failed_count
FROM metrics.breakdown_kafka;

--  TOPN
CREATE TABLE IF NOT EXISTS metrics.topn_kafka (
    window_start    DateTime64(3),
    window_end      DateTime64(3),
    rank_num        UInt8,
    account_id      UInt64,
    tx_count        UInt64,
    total_value     Decimal(19, 4)
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'metrics.topn',
    kafka_group_name = 'clickhouse-topn',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS metrics.topn_mv
TO metrics.topn AS
SELECT
    window_start, window_end, rank_num, account_id, tx_count, total_value
FROM metrics.topn_kafka;