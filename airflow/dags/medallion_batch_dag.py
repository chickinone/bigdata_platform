"""FILE SINH TỰ ĐỘNG - đừng sửa tay. Nguồn: metadata/pipelines/batch/. Sinh lại: python -m dataplatform.cli write.

DAG orchestration medallion batch (Bronze -> Silver -> Gold/Iceberg). Thứ tự task suy
từ phụ thuộc input/output của batch spec. Mỗi task = spark-submit trong container
spark-master (Airflow cần docker CLI + socket; xem airflow/README.md).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Chính sách vận hành mặc định. Đổi ở đây = đổi cho mọi task (một chỗ).
default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=2),
}


with DAG(
    dag_id="medallion_batch",
    description="Medallion Spark batch — sinh từ metadata (ADR-0031).",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["medallion", "spark", "generated"],
) as dag:
    silver_enriched_transactions = BashOperator(
        task_id="silver_enriched_transactions",
        bash_command="docker exec -e JOB_PLAN=/opt/spark-jobs/generated/silver_enriched_transactions.json bigdata-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/medallion_runner.py",
    )
    gold_customer_lifetime_metrics = BashOperator(
        task_id="gold_customer_lifetime_metrics",
        bash_command="docker exec -e JOB_PLAN=/opt/spark-jobs/generated/gold_customer_lifetime_metrics.json bigdata-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/medallion_runner.py",
    )
    gold_daily_transaction_summary = BashOperator(
        task_id="gold_daily_transaction_summary",
        bash_command="docker exec -e JOB_PLAN=/opt/spark-jobs/generated/gold_daily_transaction_summary.json bigdata-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/medallion_runner.py",
    )
    gold_high_risk_transactions = BashOperator(
        task_id="gold_high_risk_transactions",
        bash_command="docker exec -e JOB_PLAN=/opt/spark-jobs/generated/gold_high_risk_transactions.json bigdata-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/medallion_runner.py",
    )
    iceberg_silver_enriched = BashOperator(
        task_id="iceberg_silver_enriched",
        bash_command="docker exec -e JOB_PLAN=/opt/spark-jobs/generated/iceberg_silver_enriched.json bigdata-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/medallion_runner.py",
    )

    # Phụ thuộc suy từ input/output của batch spec.
    silver_enriched_transactions >> [gold_customer_lifetime_metrics, gold_daily_transaction_summary, gold_high_risk_transactions, iceberg_silver_enriched]
