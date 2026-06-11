import os
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # 1 partition Kafka, parallelism=1 đủ
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
            'properties.group.id' = 'flink-lane1-breakdown',
            'value.format' = 'avro-confluent',
            'value.avro-confluent.url' = 'http://schema-registry:8081',
            'scan.startup.mode' = 'earliest-offset'
        )
    """)

    tenv.execute_sql("""
        CREATE TABLE breakdown_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            tx_type STRING,
            tx_count BIGINT,
            total_value DECIMAL(19, 4),
            success_count BIGINT,
            failed_count BIGINT
        ) WITH (
            'connector' = 'print'
        )
    """)

    tenv.execute_sql("""
        INSERT INTO breakdown_sink
        SELECT
            window_start,
            window_end,
            `after`.transaction_type AS tx_type,
            COUNT(*) AS tx_count,
            SUM(CAST(`after`.amount AS DECIMAL(19, 4))) AS total_value,
            COUNT(*) FILTER (WHERE `after`.status = 'completed') AS success_count,
            COUNT(*) FILTER (WHERE `after`.status = 'failed') AS failed_count
        FROM TABLE(
            CUMULATE(
                TABLE transactions_source,
                DESCRIPTOR(event_time),
                INTERVAL '5' MINUTE,
                INTERVAL '1' DAY
            )
        )
        WHERE op = 'c'
        GROUP BY window_start, window_end, `after`.transaction_type
    """)


if __name__ == "__main__":
    main()