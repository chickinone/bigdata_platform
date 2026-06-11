import os
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import json
from pyflink.common.typeinfo import Types
from pyflink.common.time import Time
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ListStateDescriptor
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)

def main():
    #Environment setup 
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(30_000, CheckpointingMode.EXACTLY_ONCE)

    jar_dir = "/opt/flink/jobs/jars"
    env.add_jars(
        f"file://{jar_dir}/flink-sql-connector-kafka-3.1.0-1.18.jar",
        f"file://{jar_dir}/flink-sql-avro-confluent-registry-1.18.1.jar",
    )

    tenv = StreamTableEnvironment.create(
        env,
        environment_settings=EnvironmentSettings.in_streaming_mode()
    )
    tenv.get_config().set("table.exec.source.idle-timeout", "5000 ms")

    # Đọc Kafka CDC topic bằng Table API 
    # Schema match với Avro schema của bankdb.public.transactions
    tenv.execute_sql("""
        CREATE TABLE transactions_source (
            op STRING,
            ts_ms BIGINT,
            `after` ROW<transaction_id BIGINT, account_id BIGINT, transaction_type STRING, amount STRING, currency STRING, status STRING>,
            event_time AS TO_TIMESTAMP_LTZ(ts_ms, 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'bankdb.public.transactions',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink-lane3-fraud',
            'value.format' = 'avro-confluent',
            'value.avro-confluent.url' = 'http://schema-registry:8081',
            'scan.startup.mode' = 'latest-offset'
        )
    """)

    # Filter INSERTs (op='c') và flatten ra cột top-level 
    # Convert nested ROW thành flat columns để dễ xử lý ở DataStream
    flattened = tenv.sql_query("""
        SELECT
            `after`.transaction_id AS transaction_id,
            `after`.account_id AS account_id,
            `after`.transaction_type AS tx_type,
            `after`.amount AS amount,
            `after`.currency AS currency,
            `after`.status AS status,
            event_time
        FROM transactions_source
        WHERE op = 'c'
    """)

    # Convert Table → DataStream 
    ds = tenv.to_data_stream(flattened)

    # Sink tạm: in ra console để verify pipeline đọc được 
    # L3.1 debug: in raw event ra console 
    ds.print("LANE3-RAW").name("debug-print")

    # 2: Velocity Fraud Detector 
    velocity_alerts = (
        ds
        .key_by(lambda row: row.account_id, key_type=Types.LONG())
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(VelocityDetector(threshold=5), output_type=Types.STRING())
        .name("velocity-detector")
    )


    # L3.3: Failed Storm Detector 
    storm_alerts = (
        ds
        .key_by(lambda row: row.account_id, key_type=Types.LONG())
        .process(
            FailedStormDetector(window_minutes=5, threshold=15),
            output_type=Types.STRING()
        )
        .name("failed-storm-detector")
    )

    # ===== L3.4: Union alerts + sink to Kafka =====
    
    # Gộp 2 alert stream thành 1
    all_alerts = velocity_alerts.union(storm_alerts)
    
    # Kafka sink
    kafka_alert_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("fraud-alerts")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    
    all_alerts.sink_to(kafka_alert_sink).name("kafka-alert-sink")
    
    # Vẫn print để debug (có thể bỏ sau)
    all_alerts.print("ALERT").name("debug-print")

    # Submit
    env.execute("lane3_fraud_detection")

class VelocityDetector(ProcessWindowFunction):
    """
    Đếm transactions trong tumbling window 1 phút per account.
    Emit alert nếu count > threshold.
    """

    def __init__(self, threshold: int):
        super().__init__()
        self.threshold = threshold

    def process(self, key, ctx, elements):
        # key = account_id (int)
        # elements = iterable các Row trong window
        # ctx.window() có .start và .end (millis)
        count = sum(1 for _ in elements)

        if count > self.threshold:
            alert = {
                "alert_type": "VELOCITY_FRAUD",
                "severity": "MEDIUM",
                "account_id": int(key),
                "tx_count": count,
                "threshold": self.threshold,
                "window_start_ms": ctx.window().start,
                "window_end_ms": ctx.window().end,
            }
            yield json.dumps(alert)

class FailedStormDetector(KeyedProcessFunction):
    """
    Detect: >= N failed transactions trong sliding 5-phút window per account.
    
    Khác với TumblingWindow: state là LIST sliding theo từng event,
    không reset cứng tại biên 5 phút.
    """

    def __init__(self, window_minutes: int, threshold: int):
        super().__init__()
        self.window_ms = window_minutes * 60 * 1000
        self.threshold = threshold

    def open(self, runtime_context):
        # ListState: lưu tuple (timestamp_ms, transaction_id, amount)
        # PyFlink yêu cầu type info rõ ràng cho tuple
        descriptor = ListStateDescriptor(
            "failed_history",
            Types.TUPLE([Types.LONG(), Types.LONG(), Types.STRING()])
        )
        self.failed_history = runtime_context.get_list_state(descriptor)

    def process_element(self, value, ctx):
        # value là Row có cột: transaction_id, account_id, tx_type, amount, currency, status, event_time
        # Chỉ care về failed transactions
        if value.status != "failed":
            return

        current_ts_ms = ctx.timestamp()  # event-time của record này
        if current_ts_ms is None:
            return  # bỏ qua nếu không có timestamp

        # lấy history hiện tại
        history = list(self.failed_history.get())

        # cleanup entries quá cũ (> 5 phút trước current)
        cutoff = current_ts_ms - self.window_ms
        history = [entry for entry in history if entry[0] >= cutoff]

        # thêm event mới
        history.append((current_ts_ms, int(value.transaction_id), str(value.amount)))

        # Update state
        self.failed_history.update(history)

        # Check threshold
        if len(history) >= self.threshold:
            alert = {
                "alert_type": "FAILED_STORM",
                "severity": "HIGH",
                "account_id": int(ctx.get_current_key()),
                "failure_count_window": len(history),
                "threshold": self.threshold,
                "window_minutes": self.window_ms // 60000,
                "detected_at_ms": current_ts_ms,
                "recent_failures": [
                    {"ts_ms": e[0], "tx_id": e[1], "amount": e[2]}
                    for e in history[-self.threshold:]  # chỉ N gần nhất
                ],
            }
            yield json.dumps(alert)

        # set timer để cleanup nếu account này im lặng
        # Timer này fire khi watermark vượt qua (current + window_ms)
        ctx.timer_service().register_event_time_timer(current_ts_ms + self.window_ms)

    def on_timer(self, timestamp, ctx):
        """Cleanup state khi timer fire — đảm bảo state không phình mãi."""
        history = list(self.failed_history.get())
        cutoff = timestamp - self.window_ms
        kept = [entry for entry in history if entry[0] >= cutoff]

        if kept:
            self.failed_history.update(kept)
        else:
            self.failed_history.clear()  # account này im lặng quá lâu → xóa hết state

if __name__ == "__main__":
    main()