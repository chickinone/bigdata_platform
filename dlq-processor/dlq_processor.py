"""DLQ processor — biến lỗi của Kafka Connect thành dữ liệu truy vấn được.

Luồng:

    sink lỗi ──> dlq.<connector>  (Kafka Connect tự ghi + header __connect.errors.*)
                      │
                      ▼
              dlq-processor        (phân loại TRANSIENT / PERMANENT / UNKNOWN)
                      │
                      ▼
                 dlq.events        (Kafka, JSON đã enrich)
                      │
                      ▼
        metrics.dlq_events_kafka -> _mv -> metrics.dlq_events  (ClickHouse)
                      │
                      ▼
                   Grafana

Hai quyết định thiết kế đáng chú ý (chi tiết ở docs/decisions/0017-*.md):

1. **Ghi qua Kafka, không INSERT thẳng ClickHouse.** DLQ event là thứ ta cần
   nhất đúng lúc hệ thống đang hỏng. Nếu ClickHouse cũng đang sập mà ta INSERT
   thẳng, ta mất bản ghi lỗi ngay tại thời điểm cần nó nhất.

2. **KHÔNG tự động replay.** Bản trước đây gửi message lỗi ngược về *topic gốc*.
   Với `bankdb.public.transactions`, topic đó cũng là nguồn của Flink — nên một
   lỗi ES tạm thời sẽ khiến giao dịch **bị đếm lại** và làm sai dashboard. Ta ghi
   nhận và PARK; việc phát lại là quyết định của con người, có công cụ riêng.
"""
import datetime
import json
import logging
import os
import time
from collections import defaultdict
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("dlq-processor")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
EVENTS_TOPIC = os.getenv("DLQ_EVENTS_TOPIC", "dlq.events")
INVENTORY_FILE = os.getenv("DLQ_INVENTORY_FILE", "dlq_topics.json")

# Lỗi hạ tầng tạm thời — thử lại có ý nghĩa (nhưng xem ghi chú về replay ở trên).
TRANSIENT = {
    "org.apache.kafka.connect.errors.RetriableException",
    "java.net.ConnectException",
    "java.net.SocketTimeoutException",
    "org.elasticsearch.client.ResponseException",
}
# Dữ liệu/schema hỏng — thử lại bao nhiêu lần cũng hỏng y hệt.
PERMANENT = {
    "org.apache.kafka.connect.errors.DataException",
    "org.apache.kafka.common.errors.SerializationException",
    "org.apache.kafka.connect.errors.SchemaException",
}


def load_dlq_topics():
    """Đọc bản kê topic DLQ do control plane sinh ra.

    Trước đây danh sách này được HARDCODE ngay trong file này, tách rời khỏi
    cấu hình connector — thêm connector mà quên sửa list thì lỗi của nó rơi vào
    hư không (metadata sprawl #12). Nay nó được sinh từ metadata/datasets/*.yaml.
    """
    path = Path(__file__).parent / INVENTORY_FILE
    if not path.exists():
        raise SystemExit(
            f"Thiếu {path}. Sinh lại bằng: python -m dataplatform.cli write"
        )

    inventory = json.loads(path.read_text(encoding="utf-8"))
    entries = inventory["connectors"]
    topics = [e["dlq_topic"] for e in entries]
    log.info(f"Nạp {len(topics)} topic DLQ từ bản kê sinh tự động:")
    for e in entries:
        log.info(f"  {e['dlq_topic']:<28} <- {e['connector']}")
    return topics


def categorize(exception_class):
    if exception_class in TRANSIENT:
        return "TRANSIENT"
    if exception_class in PERMANENT:
        return "PERMANENT"
    return "UNKNOWN"


def parse_headers(headers):
    """Bóc header __connect.errors.* mà Kafka Connect gắn vào mỗi bản ghi DLQ.

    Chỉ có khi connector bật errors.deadletterqueue.context.headers.enable=true.
    Thiếu nó thì mọi lỗi rơi vào UNKNOWN và việc phân loại thành vô nghĩa.
    """
    meta = {}
    if not headers:
        return meta
    for key, value in headers:
        if key and key.startswith("__connect.errors."):
            field = key.replace("__connect.errors.", "")
            try:
                meta[field] = value.decode("utf-8", errors="replace") if value else ""
            except Exception:
                meta[field] = "<binary>"
    return meta


def decide_action(category):
    """Quyết định làm gì với bản ghi lỗi.

    Hiện MỌI nhóm đều PARKED — ghi nhận rồi dừng, chờ người xử lý.

    Vì sao TRANSIENT cũng không tự replay: đích replay đúng phải là một topic
    CHỈ connector đó đọc. Topic gốc không thoả (Flink cũng đọc). Xây topic retry
    riêng thì vướng: ES sink lấy tên index TỪ tên topic, còn S3 sink lấy đường
    dẫn partition từ tên topic — nên phải thêm RegexRouter cho từng connector.
    Đó là việc riêng, làm nửa vời còn tệ hơn không làm.

    Trong lúc chờ: connector Kafka Connect đã tự retry vài lần trước khi đẩy vào
    DLQ, nên phần lớn lỗi thoáng qua đã được xử lý trước khi tới đây.
    """
    return "PARKED"


def build_event(msg, meta, category, action):
    """Dựng bản ghi enrich để đẩy sang ClickHouse.

    Khoá phải KHỚP TUYỆT ĐỐI cột của metrics.dlq_events_kafka. Lệch một khoá là
    Materialized View bỏ dòng đó mà KHÔNG báo lỗi — chế độ hỏng tệ nhất của
    ClickHouse (xem ADR-0007).
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    return {
        "detected_at": now.strftime("%Y-%m-%d %H:%M:%S.") + f"{now.microsecond // 1000:03d}",
        "connector_name": meta.get("connector.name", "unknown"),
        "dlq_topic": msg.topic,
        "original_topic": meta.get("topic", "unknown"),
        "category": category,
        "action": action,
        "error_class": meta.get("exception.class.name", "unknown"),
        "error_stage": meta.get("stage", "unknown"),
        # Cắt ngắn: stack trace có thể dài hàng KB, và ta lưu 30 ngày.
        "error_message": (meta.get("exception.message", "") or "")[:2000],
        # Chỉ lưu KHOÁ, không lưu nội dung message: customers chứa PII
        # (full_name/email/phone). Nội dung gốc vẫn nằm trong topic DLQ nếu cần
        # điều tra — không cần nhân bản nó sang ClickHouse.
        "message_key": (msg.key.decode("utf-8", errors="replace") if msg.key else ""),
        # Vị trí trong TOPIC DLQ — khoá chống trùng của ReplacingMergeTree.
        "dlq_partition": msg.partition,
        "dlq_offset": msg.offset,
        # Vị trí trong TOPIC GỐC — thứ người vận hành cần để TÌM LẠI bản ghi lỗi
        # mà phát lại. Hai cái này khác nhau; trộn chúng làm một là mất đường về.
        # -1 = header không có (connector chưa bật context.headers.enable).
        "original_partition": _to_int(meta.get("partition"), -1),
        "original_offset": _to_int(meta.get("offset"), -1),
        "message_size": len(msg.value) if msg.value else 0,
    }


def _to_int(value, default):
    """Header Kafka luôn là bytes/chuỗi. Thiếu hoặc hỏng thì trả về mặc định,
    chứ không để một header lạ làm chết cả processor.
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def connect_with_retry(topics, max_attempts=10):
    for attempt in range(max_attempts):
        try:
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id="dlq-processor-v3",
                auto_offset_reset="earliest",
                value_deserializer=lambda v: v,
                enable_auto_commit=True,
                consumer_timeout_ms=-1,
            )
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",
            )
            log.info("Đã kết nối Kafka")
            return consumer, producer
        except NoBrokersAvailable:
            log.warning(f"Kafka chưa sẵn sàng, thử lại {attempt + 1}/{max_attempts}...")
            time.sleep(5)
    raise RuntimeError("Không kết nối được Kafka sau nhiều lần thử")


def main():
    topics = load_dlq_topics()
    consumer, producer = connect_with_retry(topics)
    stats = defaultdict(int)
    last_stats = time.time()

    log.info(f"DLQ processor chạy — theo dõi {len(topics)} topic, đẩy sang {EVENTS_TOPIC}")

    for msg in consumer:
        meta = parse_headers(msg.headers)
        category = categorize(meta.get("exception.class.name", "unknown"))
        action = decide_action(category)
        event = build_event(msg, meta, category, action)

        stats[f"{msg.topic}|{category}"] += 1
        stats[f"TOTAL|{category}"] += 1

        try:
            producer.send(EVENTS_TOPIC, value=event)
            stats["TOTAL|PUBLISHED"] += 1
        except Exception as e:
            # Không nuốt lỗi im lặng: mất DLQ event nghĩa là mất khả năng quan sát.
            log.error(f"Không đẩy được sang {EVENTS_TOPIC}: {e}")
            stats["TOTAL|PUBLISH_FAILED"] += 1

        log.info(
            f"DLQ: connector={event['connector_name']} "
            f"topic_goc={event['original_topic']} stage={event['error_stage']} "
            f"loi={event['error_class']} nhom={category} -> {action} "
            f"(offset={msg.offset})"
        )

        if category == "PERMANENT":
            log.warning("  → lỗi VĨNH VIỄN: dữ liệu/schema hỏng, thử lại vô ích. Cần người xem.")
        elif category == "TRANSIENT":
            log.warning("  → lỗi TẠM THỜI: đã park. Sửa hạ tầng rồi phát lại thủ công.")
        else:
            log.warning(f"  → CHƯA PHÂN LOẠI: {event['error_class']} — cân nhắc bổ sung vào TRANSIENT/PERMANENT.")

        if time.time() - last_stats > 30:
            log.info("=== Thống kê ===")
            for key in sorted(stats):
                log.info(f"  {key}: {stats[key]}")
            last_stats = time.time()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Đang dừng...")
