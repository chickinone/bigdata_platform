# FILE SINH TỰ ĐỘNG - đừng sửa tay. Sinh lại: python -m dataplatform.cli write
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
        bash_command="docker exec -e JOB_PLAN=/opt/spark-jobs/generated/silver_enriched_transactions.json -e AS_OF={{ds}} bigdata-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/medallion_runner.py 2>&1 | tee /tmp/medallion_silver_enriched_transactions.$$.log; grep -q '^WROTE ' /tmp/medallion_silver_enriched_transactions.$$.log",
    )
    gold_customer_lifetime_metrics = BashOperator(
        task_id="gold_customer_lifetime_metrics",
        bash_command="docker exec -e JOB_PLAN=/opt/spark-jobs/generated/gold_customer_lifetime_metrics.json -e AS_OF={{ds}} bigdata-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/medallion_runner.py 2>&1 | tee /tmp/medallion_gold_customer_lifetime_metrics.$$.log; grep -q '^WROTE ' /tmp/medallion_gold_customer_lifetime_metrics.$$.log",
    )
    gold_daily_transaction_summary = BashOperator(
        task_id="gold_daily_transaction_summary",
        bash_command="docker exec -e JOB_PLAN=/opt/spark-jobs/generated/gold_daily_transaction_summary.json -e AS_OF={{ds}} bigdata-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/medallion_runner.py 2>&1 | tee /tmp/medallion_gold_daily_transaction_summary.$$.log; grep -q '^WROTE ' /tmp/medallion_gold_daily_transaction_summary.$$.log",
    )
    gold_high_risk_transactions = BashOperator(
        task_id="gold_high_risk_transactions",
        bash_command="docker exec -e JOB_PLAN=/opt/spark-jobs/generated/gold_high_risk_transactions.json -e AS_OF={{ds}} bigdata-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/medallion_runner.py 2>&1 | tee /tmp/medallion_gold_high_risk_transactions.$$.log; grep -q '^WROTE ' /tmp/medallion_gold_high_risk_transactions.$$.log",
    )
    iceberg_silver_enriched = BashOperator(
        task_id="iceberg_silver_enriched",
        bash_command="docker exec -e JOB_PLAN=/opt/spark-jobs/generated/iceberg_silver_enriched.json -e AS_OF={{ds}} bigdata-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/medallion_runner.py 2>&1 | tee /tmp/medallion_iceberg_silver_enriched.$$.log; grep -q '^WROTE ' /tmp/medallion_iceberg_silver_enriched.$$.log",
    )

    # Phụ thuộc suy từ input/output của batch spec.
    silver_enriched_transactions >> [gold_customer_lifetime_metrics, gold_daily_transaction_summary, gold_high_risk_transactions, iceberg_silver_enriched]
