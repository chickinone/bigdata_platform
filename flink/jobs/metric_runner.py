"""Runner metric tổng quát — THỰC THI job plan sinh từ metadata.

Đây là "data plane" của Pha 3: nó KHÔNG chứa logic pipeline nào cả. Toàn bộ SQL
(source DDL, sink DDL, INSERT) được sinh trên host bởi
`dataplatform/generators/flink_sql.py` từ pipeline spec + contract, rồi ghi ra một
job plan JSON. Runner này chỉ đọc plan và submit.

Tách bạch như vậy để: (1) container Flink không cần jsonschema/pyyaml/registry;
(2) thêm/sửa metric = sửa YAML trên host, KHÔNG đụng file Python này. Nó thay
lane1_dashboard.py: 4 INSERT + source ROW + 4 sink DDL viết tay giờ đều sinh ra.
"""
import json
import os

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


def main():
    plan_path = os.getenv("JOB_PLAN", "/opt/flink/jobs/generated/metrics-job.json")
    with open(plan_path, encoding="utf-8") as f:
        plan = json.load(f)

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

    # 1 source (share cho mọi pipeline) + N sink, đều sinh từ contract.
    tenv.execute_sql(plan["source_ddl"])
    for ddl in plan["sink_ddls"]:
        tenv.execute_sql(ddl)

    # Gộp mọi INSERT vào 1 StatementSet -> 1 execution graph, 1 lần đọc source.
    stmt_set = tenv.create_statement_set()
    for insert in plan["inserts"]:
        stmt_set.add_insert_sql(insert)
    stmt_set.execute()


if __name__ == "__main__":
    main()
