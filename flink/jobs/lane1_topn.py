import os
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

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

    tenv.execute_sql(f"""
        CREATE TABLE transactions_source (
            op STRING,
            ts_ms BIGINT,
            `after` ROW<transaction_id BIGINT, account_id BIGINT, transaction_type STRING, amount STRING, currency STRING, status STRING>,
            event_time AS TO_TIMESTAMP_LTZ(ts_ms, 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'bankdb.public.transactions',
            'properties.bootstrap.servers' = '{kafka_bootstrap_servers}',
            'properties.group.id' = 'flink-lane1-topn',
            'value.format' = 'avro-confluent',
            'value.avro-confluent.url' = '{schema_registry_url}',
            'scan.startup.mode' = 'earliest-offset'
        )
    """)

    tenv.execute_sql("""
        CREATE TABLE topn_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            rank_num BIGINT,
            account_id BIGINT,
            tx_count BIGINT,
            total_value DECIMAL(19, 4)
        ) WITH (
            'connector' = 'print'
        )
    """)

    # Top-N pattern: 
    # 1. Inner: aggregate per (window, account_id)
    # 2. Outer: ROW_NUMBER() OVER PARTITION BY window ORDER BY total_value DESC
    # 3. Filter rn <= 10
    tenv.execute_sql("""
        INSERT INTO topn_sink
        SELECT window_start, window_end, rn AS rank_num, account_id, tx_count, total_value
        FROM (
            SELECT 
                window_start, window_end, account_id, tx_count, total_value,
                ROW_NUMBER() OVER (
                    PARTITION BY window_start, window_end 
                    ORDER BY total_value DESC
                ) AS rn
            FROM (
                SELECT 
                    window_start, window_end,
                    `after`.account_id AS account_id,
                    COUNT(*) AS tx_count,
                    SUM(CAST(`after`.amount AS DECIMAL(19, 4))) AS total_value
                FROM TABLE(
                    CUMULATE(
                        TABLE transactions_source,
                        DESCRIPTOR(event_time),
                        INTERVAL '5' MINUTE,
                        INTERVAL '1' DAY
                    )
                )
                WHERE op = 'c'
                GROUP BY window_start, window_end, `after`.account_id
            )
        )
        WHERE rn <= 10
    """)

if __name__ == "__main__":
    main()
