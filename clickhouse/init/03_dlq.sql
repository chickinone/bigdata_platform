-- =====================================================================
-- DLQ EVENTS — nơi lỗi của Kafka Connect trở thành DỮ LIỆU truy vấn được
--
-- Luồng:  sink lỗi -> dlq.<connector> -> dlq-processor (phân loại)
--                  -> dlq.events (Kafka) -> _kafka -> _mv -> dlq_events
--
-- Vì sao đi qua Kafka thay vì để dlq-processor INSERT thẳng HTTP:
--   1. Đúng pattern đã chốt ở ADR-0007 (Flink cũng không ghi thẳng ClickHouse).
--   2. Quan trọng hơn: DLQ event là thứ ta CẦN NHẤT đúng lúc hệ thống đang hỏng.
--      Nếu ClickHouse sập mà processor INSERT thẳng, ta MẤT bản ghi lỗi ngay tại
--      thời điểm cần nó nhất. Qua Kafka thì chúng nằm chờ, ClickHouse lên là đuổi kịp.
--   3. ClickHouse ghét insert nhỏ lẻ; Kafka engine gom thành block lớn.
-- =====================================================================

-- Bảng đích: lưu thật, Grafana đọc bảng này.
CREATE TABLE IF NOT EXISTS metrics.dlq_events (
    detected_at       DateTime64(3),
    connector_name    LowCardinality(String),
    dlq_topic         LowCardinality(String),
    original_topic    LowCardinality(String),
    -- TRANSIENT: lỗi hạ tầng tạm thời (ES sập, timeout) -> thử lại có ý nghĩa.
    -- PERMANENT: dữ liệu/schema hỏng -> thử lại bao nhiêu lần cũng hỏng.
    -- UNKNOWN:   chưa phân loại được -> mặc định coi như không an toàn để thử lại.
    category          LowCardinality(String),
    -- Hành động đã thực hiện. PARKED = ghi nhận và dừng, chờ người xử lý.
    action            LowCardinality(String),
    error_class       LowCardinality(String),
    error_stage       LowCardinality(String),
    error_message     String,
    message_key       String,
    -- HAI cặp vị trí khác nhau, đừng trộn:
    --   dlq_*      = vị trí bản ghi TRONG TOPIC DLQ  -> khoá chống trùng
    --   original_* = vị trí bản ghi TRONG TOPIC GỐC  -> để tìm lại mà phát lại
    dlq_partition      UInt32,
    dlq_offset         UInt64,
    original_partition Int32 DEFAULT -1,
    original_offset    Int64 DEFAULT -1,
    message_size      UInt32,
    inserted_at       DateTime DEFAULT now()
)
-- ReplacingMergeTree + ORDER BY (dlq_topic, dlq_partition, dlq_offset):
-- bộ ba đó là KHOÁ TỰ NHIÊN DUY NHẤT của một message Kafka. Nhờ vậy nếu
-- dlq-processor restart và đọc lại (at-least-once), bản trùng sẽ được gộp
-- thay vì đếm hai lần.
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMMDD(detected_at)
ORDER BY (dlq_topic, dlq_partition, dlq_offset)
TTL toDateTime(detected_at) + INTERVAL 30 DAY;

-- Bảng đệm: Kafka engine. ĐỌC MỘT LẦN LÀ MẤT — đừng SELECT thẳng vào nó khi
-- MV đang chạy, sẽ cướp dữ liệu của MV.
CREATE TABLE IF NOT EXISTS metrics.dlq_events_kafka (
    detected_at       DateTime64(3),
    connector_name    String,
    dlq_topic         String,
    original_topic    String,
    category          String,
    action            String,
    error_class       String,
    error_stage       String,
    error_message     String,
    message_key       String,
    dlq_partition      UInt32,
    dlq_offset         UInt64,
    original_partition Int32,
    original_offset    Int64,
    message_size      UInt32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'dlq.events',
    kafka_group_name = 'clickhouse-dlq-events',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    -- Một message DLQ hỏng không được làm chết cả consumer. Cho phép bỏ qua
    -- vài dòng lỗi thay vì để bảng dlq_events ngừng nhận hoàn toàn.
    kafka_skip_broken_messages = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS metrics.dlq_events_mv
TO metrics.dlq_events AS
SELECT
    detected_at, connector_name, dlq_topic, original_topic,
    category, action, error_class, error_stage, error_message,
    message_key, dlq_partition, dlq_offset,
    original_partition, original_offset, message_size
FROM metrics.dlq_events_kafka;
