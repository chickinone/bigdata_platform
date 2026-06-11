import os
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    # Stream environment + checkpointing
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    # Checkpoint mỗi 30s → mất máy resume từ chính xác chỗ đó
    env.enable_checkpointing(30_000, CheckpointingMode.EXACTLY_ONCE)
    jar_dir = "/opt/flink/jobs/jars"
    env.add_jars(
        f"file://{jar_dir}/flink-sql-connector-kafka-3.1.0-1.18.jar",
        f"file://{jar_dir}/flink-sql-avro-confluent-registry-1.18.1.jar",
    )

    # Table environment cho source declarative
    tenv = StreamTableEnvironment.create(
        env,
        environment_settings=EnvironmentSettings.in_streaming_mode()
    )
    tenv.get_config().set("table.exec.source.idle-timeout", "5000 ms")


    # Source: Kafka topic Avro + Confluent registry
    #    - event_time computed từ ts_ms (Debezium envelope top-level)
    #    - watermark = event_time - 5s (cho phép out-of-orderness 5s)
    #    - earliest-offset: đảm bảo đọc hết data cũ khi job mới chạy, không bị miss data
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
            'properties.group.id' = 'flink-lane1-timeseries',
            'value.format' = 'avro-confluent',
            'value.avro-confluent.url' = 'http://schema-registry:8081',
            'scan.startup.mode' = 'earliest-offset'
        )
    """)

    tenv.execute_sql("""
        CREATE TABLE timeseries_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            tx_type STRING,
            tx_count BIGINT,
            total_amount DECIMAL(19, 4)
        ) WITH (
            'connector' = 'print'
        )
    """)
    # Pipeline: tumbling 1-min window, group by transaction_type
    #    - TUMBLE TVF: cửa sổ 1 phút không chồng theo event_time
    #    - WHERE op='c': chỉ lấy INSERT mới, skip snapshot ('r') và update ('u')
    #    - Cast amount string → DECIMAL để sum chính xác (giữ precision tiền tệ)
    tenv.execute_sql("""
        INSERT INTO timeseries_sink
        SELECT
            window_start,
            window_end,
            `after`.transaction_type AS tx_type,
            COUNT(*) AS tx_count,
            SUM(CAST(`after`.amount AS DECIMAL(19, 4))) AS total_amount
        FROM TABLE(
            TUMBLE(
                TABLE transactions_source,
                DESCRIPTOR(event_time),
                INTERVAL '1' MINUTE
            )
        )
        WHERE op = 'c'
        GROUP BY window_start, window_end, `after`.transaction_type
    """)

if __name__ == "__main__":
    main()