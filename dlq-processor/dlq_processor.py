import logging
import os
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger('dlq-processor')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_PORT = os.getenv('CLICKHOUSE_PORT', '8123')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')

DLQ_TOPICS = [
    'dlq.es-sink-customers',
    'dlq.es-sink-accounts',
    'dlq.es-sink-transactions',
    'dlq.es-sink-transfers',
    'dlq.es-sink-fraud-alerts',
    'dlq.s3-sink-cdc',
]

TRANSIENT = {
    'org.apache.kafka.connect.errors.RetriableException',
    'java.net.ConnectException',
    'java.net.SocketTimeoutException',
    'org.elasticsearch.client.ResponseException',
}
PERMANENT = {
    'org.apache.kafka.connect.errors.DataException',
    'org.apache.kafka.common.errors.SerializationException',
    'org.apache.kafka.connect.errors.SchemaException',
}

CLICKHOUSE_URL = (
    f'http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?'
    f'user={urllib.parse.quote(CLICKHOUSE_USER)}'
    f'&password={urllib.parse.quote(CLICKHOUSE_PASSWORD)}'
)


def categorize(exception_class):
    if exception_class in TRANSIENT:
        return 'TRANSIENT'
    if exception_class in PERMANENT:
        return 'PERMANENT'
    return 'UNKNOWN'


def parse_headers(headers):
    meta = {}
    if not headers:
        return meta
    for key, value in headers:
        if key and key.startswith('__connect.errors.'):
            field = key.replace('__connect.errors.', '')
            try:
                meta[field] = value.decode('utf-8', errors='replace') if value else ''
            except Exception:
                meta[field] = '<binary>'
    return meta


def esc_sql(s):
    """Escape single quotes for SQL string literal."""
    return str(s).replace("'", "\\'")


def write_to_clickhouse(dlq_topic, original_topic, connector_name,
                        error_class, error_stage, category, offset, msg_size):
    sql = (
        "INSERT INTO metrics.dlq_events "
        "(dlq_topic, original_topic, connector_name, error_class, error_stage, category, offset, message_size) "
        f"VALUES ('{esc_sql(dlq_topic)}', '{esc_sql(original_topic)}', '{esc_sql(connector_name)}', "
        f"'{esc_sql(error_class)}', '{esc_sql(error_stage)}', '{esc_sql(category)}', {offset}, {msg_size})"
    )
    try:
        url = CLICKHOUSE_URL + '&query=' + urllib.parse.quote(sql)
        req = urllib.request.Request(url, method='POST')
        urllib.request.urlopen(req, timeout=5).read()
    except Exception as e:
        log.error(f'ClickHouse write failed: {e}')


def connect_with_retry(max_attempts=10):
    for attempt in range(max_attempts):
        try:
            consumer = KafkaConsumer(
                *DLQ_TOPICS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id='dlq-processor-v2',  # bump version để re-read from earliest
                auto_offset_reset='earliest',
                value_deserializer=lambda v: v,
                enable_auto_commit=True,
                consumer_timeout_ms=-1,
            )
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: v,
            )
            log.info('Connected to Kafka')
            return consumer, producer
        except NoBrokersAvailable:
            log.warning(f'Kafka not ready, retry {attempt + 1}/{max_attempts}...')
            time.sleep(5)
    raise RuntimeError('Cannot connect to Kafka after retries')


def main():
    consumer, producer = connect_with_retry()
    stats = defaultdict(int)
    last_stats = time.time()
    
    log.info(f'DLQ processor started, monitoring {len(DLQ_TOPICS)} topics')
    
    for msg in consumer:
        meta = parse_headers(msg.headers)
        error_class = meta.get('exception.class.name', 'unknown')
        original_topic = meta.get('topic', 'unknown')
        connector_name = meta.get('connector.name', 'unknown')
        stage = meta.get('stage', 'unknown')
        category = categorize(error_class)
        msg_size = len(msg.value) if msg.value else 0
        
        stats[f'{msg.topic}|{category}'] += 1
        stats[f'TOTAL|{category}'] += 1
        
        log.info(
            f'DLQ event: dlq_topic={msg.topic}, original_topic={original_topic}, '
            f'stage={stage}, error_class={error_class}, category={category}, offset={msg.offset}'
        )
        
        # Write to ClickHouse for Grafana
        write_to_clickhouse(
            dlq_topic=msg.topic,
            original_topic=original_topic,
            connector_name=connector_name,
            error_class=error_class,
            error_stage=stage,
            category=category,
            offset=msg.offset,
            msg_size=msg_size,
        )
        
        # Action by category
        if category == 'TRANSIENT':
            try:
                producer.send(original_topic, value=msg.value, headers=msg.headers)
                stats[f'{msg.topic}|REPLAYED'] += 1
                log.info(f'  → Auto-replayed to {original_topic}')
            except Exception as e:
                log.error(f'  → Replay failed: {e}')
        elif category == 'PERMANENT':
            log.warning(f'  → PERMANENT error, manual review needed')
        else:
            log.warning(f'  → UNKNOWN error: {error_class}')
        
        if time.time() - last_stats > 30:
            log.info('=== Stats ===')
            for key in sorted(stats.keys()):
                log.info(f'  {key}: {stats[key]}')
            log.info('=============')
            last_stats = time.time()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        log.info('Shutting down...')
