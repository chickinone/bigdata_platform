import json
import os

from pyflink.common import Types
from pyflink.common.time import Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction, KeyedProcessFunction
from pyflink.datastream.state import ListStateDescriptor
from pyflink.datastream.connectors.kafka import (
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


def main():
    cfg_path = os.getenv("FRAUD_CONFIG", "/opt/flink/jobs/generated/fraud-job.json")
    with open(cfg_path, encoding="utf-8") as f:
        cfg = json.load(f)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(30_000, CheckpointingMode.EXACTLY_ONCE)

    jar_dir = "/opt/flink/jobs/jars"
    env.add_jars(
        f"file://{jar_dir}/flink-sql-connector-kafka-3.1.0-1.18.jar",
        f"file://{jar_dir}/flink-sql-avro-confluent-registry-1.18.1.jar",
    )

    tenv = StreamTableEnvironment.create(
        env, environment_settings=EnvironmentSettings.in_streaming_mode()
    )
    tenv.get_config().set("table.exec.source.idle-timeout", "5000 ms")

    # Source DDL SINH từ contract (không còn ROW viết tay).
    tenv.execute_sql(cfg["source_ddl"])

    # Flatten envelope -> cột phẳng để DataStream xử lý. Coupled với tên field detector
    # dùng, nên giữ ở đây (không sinh).
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
    ds = tenv.to_data_stream(flattened)

    # Velocity: > threshold giao dịch / account trong cửa sổ tumbling.
    velocity_alerts = (
        ds
        .key_by(lambda row: row.account_id, key_type=Types.LONG())
        .window(TumblingEventTimeWindows.of(Time.minutes(cfg["velocity_window_minutes"])))
        .process(VelocityDetector(threshold=cfg["velocity_threshold"]), output_type=Types.STRING())
        .name("velocity-detector")
    )

    # Failed storm: >= threshold giao dịch 'failed' / account trong cửa sổ trượt.
    storm_alerts = (
        ds
        .key_by(lambda row: row.account_id, key_type=Types.LONG())
        .process(
            FailedStormDetector(
                window_minutes=cfg["storm_window_minutes"],
                threshold=cfg["storm_threshold"],
            ),
            output_type=Types.STRING(),
        )
        .name("failed-storm-detector")
    )

    all_alerts = velocity_alerts.union(storm_alerts)

    kafka_alert_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(cfg["sink_topic"])
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    all_alerts.sink_to(kafka_alert_sink).name("kafka-alert-sink")

    env.execute(cfg["job_name"])


class VelocityDetector(ProcessWindowFunction):
    """Đếm transaction / account trong tumbling window. Alert nếu count > threshold."""

    def __init__(self, threshold: int):
        super().__init__()
        self.threshold = threshold

    def process(self, key, ctx, elements):
        count = sum(1 for _ in elements)
        if count > self.threshold:
            yield json.dumps({
                "alert_type": "VELOCITY_FRAUD",
                "severity": "MEDIUM",
                "account_id": int(key),
                "tx_count": count,
                "threshold": self.threshold,
                "window_start_ms": ctx.window().start,
                "window_end_ms": ctx.window().end,
            })


class FailedStormDetector(KeyedProcessFunction):
    """>= N giao dịch 'failed' trong cửa sổ trượt 5 phút / account. State là LIST
    trượt theo từng event, không reset cứng tại biên.
    """

    def __init__(self, window_minutes: int, threshold: int):
        super().__init__()
        self.window_ms = window_minutes * 60 * 1000
        self.threshold = threshold

    def open(self, runtime_context):
        descriptor = ListStateDescriptor(
            "failed_history",
            Types.TUPLE([Types.LONG(), Types.LONG(), Types.STRING()]),
        )
        self.failed_history = runtime_context.get_list_state(descriptor)

    def process_element(self, value, ctx):
        if value.status != "failed":
            return
        current_ts_ms = ctx.timestamp()
        if current_ts_ms is None:
            return

        history = list(self.failed_history.get() or [])
        cutoff = current_ts_ms - self.window_ms
        history = [entry for entry in history if entry[0] >= cutoff]
        history.append((current_ts_ms, int(value.transaction_id), str(value.amount)))
        self.failed_history.update(history)

        if len(history) >= self.threshold:
            yield json.dumps({
                "alert_type": "FAILED_STORM",
                "severity": "HIGH",
                "account_id": int(ctx.get_current_key()),
                "failure_count_window": len(history),
                "threshold": self.threshold,
                "window_minutes": self.window_ms // 60000,
                "detected_at_ms": current_ts_ms,
                "recent_failures": [
                    {"ts_ms": e[0], "tx_id": e[1], "amount": e[2]}
                    for e in history[-self.threshold:]
                ],
            })

        ctx.timer_service().register_event_time_timer(current_ts_ms + self.window_ms)

    def on_timer(self, timestamp, ctx):
        """Dọn state hết hạn. KHÔNG phát alert, nhưng VẪN phải trả về iterable.

        PyFlink gọi `yield from on_timer(...)` (input_handler.py:111). Hàm này không có
        `yield` nào nên Python coi nó là hàm thường và trả `None` -> `yield from None`
        -> TypeError: 'NoneType' object is not iterable.

        Vì sao lâu mới lộ: job chạy bình thường cho tới khi timer ĐẦU TIÊN nổ (cần đủ
        giao dịch failed dồn trong cửa sổ), rồi crash -> Flink restart -> lại chạy tới
        timer sau -> crash. Vòng lặp âm thầm: `flink list` vẫn thấy RUNNING giữa hai
        lần chết, và đã lặp 245 lần / 1016 lỗi trước khi bị phát hiện.

        `process_element` không dính vì nó CÓ `yield` nên là generator sẵn.
        """
        # `get()` trả None khi state rỗng — cùng họ lỗi, chặn luôn.
        history = list(self.failed_history.get() or [])
        cutoff = timestamp - self.window_ms
        kept = [entry for entry in history if entry[0] >= cutoff]
        if kept:
            self.failed_history.update(kept)
        else:
            self.failed_history.clear()
        return []


if __name__ == "__main__":
    main()
