import os
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
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
            'properties.group.id' = 'flink-lane1-kpi',
            'value.format' = 'avro-confluent',
            'value.avro-confluent.url' = '{schema_registry_url}',
            'scan.startup.mode' = 'earliest-offset'
        )
    """)

    # Sink: 6 metric trong 1 row 
    tenv.execute_sql("""
        CREATE TABLE kpi_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            total_count BIGINT,
            total_value DECIMAL(19, 4),
            success_count BIGINT,
            failed_count BIGINT,
            success_rate DECIMAL(5, 2),
            active_users BIGINT
        ) WITH (
            'connector' = 'print'
        )
    """)

    # Pipeline: CUMULATE 5min/1day, group by day 
    # FILTER (WHERE ...) = aggregate có điều kiện, không cần CASE WHEN
    tenv.execute_sql("""
        INSERT INTO kpi_sink
        SELECT
            window_start,
            window_end,
            COUNT(*) AS total_count,
            SUM(CAST(`after`.amount AS DECIMAL(19, 4))) AS total_value,
            COUNT(*) FILTER (WHERE `after`.status = 'completed') AS success_count,
            COUNT(*) FILTER (WHERE `after`.status = 'failed') AS failed_count,
            CAST(
                COUNT(*) FILTER (WHERE `after`.status = 'completed') * 100.0 /
                NULLIF(COUNT(*), 0)
                AS DECIMAL(5, 2)
            ) AS success_rate,
            COUNT(DISTINCT `after`.account_id) AS active_users
        FROM TABLE(
            CUMULATE(
                TABLE transactions_source,
                DESCRIPTOR(event_time),
                INTERVAL '5' MINUTE,
                INTERVAL '1' DAY
            )
        )
        WHERE op = 'c'
        GROUP BY window_start, window_end
    """)

if __name__ == "__main__":
    main()
