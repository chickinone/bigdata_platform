
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as F_sum, count, countDistinct, avg, max as F_max, min as F_min,
    when, expr
)


def main():
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("S3_ACCESS_KEY", "")
    s3_secret_key = os.getenv("S3_SECRET_KEY", "")
    s3_path_style_access = os.getenv("S3_PATH_STYLE_ACCESS", "true")
    s3_ssl_enabled = os.getenv("S3_SSL_ENABLED", "false")

    spark = (
        SparkSession.builder
        .appName("build_gold_layer")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", s3_path_style_access)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", s3_ssl_enabled)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 60)
    print("READING SILVER LAYER")
    print("=" * 60)

    silver = spark.read.parquet("s3a://data-lake-silver/enriched_transactions/")
    silver = silver.withColumn("amount_dbl", col("amount").cast("double"))
    print(f"Silver rows: {silver.count():,}")
    silver.printSchema()

    # ===== GOLD 1: Daily summary per country × transaction_type =====
    print("\n" + "=" * 60)
    print("GOLD 1: daily_transaction_summary")
    print("=" * 60)

    daily_summary = (
        silver
        .groupBy("year", "month", "day", "country_code", "transaction_type")
        .agg(
            count("transaction_id").alias("txn_count"),
            F_sum("amount_dbl").alias("total_volume"),
            avg("amount_dbl").alias("avg_amount"),
            countDistinct("customer_id").alias("unique_customers"),
            F_sum(when(col("status") == "failed", 1).otherwise(0)).alias("failed_count"),
        )
        .orderBy("year", "month", "day", "country_code")
    )
    daily_summary.show(10, truncate=False)

    (
        daily_summary
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet("s3a://data-lake-gold/daily_transaction_summary/")
    )

    # ===== GOLD 2: Customer lifetime metrics =====
    print("\n" + "=" * 60)
    print("GOLD 2: customer_lifetime_metrics")
    print("=" * 60)

    customer_metrics = (
        silver
        .groupBy("customer_id", "customer_name", "country_code", "kyc_status", "risk_score")
        .agg(
            count("transaction_id").alias("total_txn_count"),
            F_sum("amount_dbl").alias("lifetime_value"),
            avg("amount_dbl").alias("avg_txn_amount"),
            F_max("posted_at").alias("last_activity"),
            F_min("posted_at").alias("first_activity"),
            countDistinct("account_id").alias("account_count"),
        )
        .orderBy(col("lifetime_value").desc())
    )
    customer_metrics.show(10, truncate=False)

    (
        customer_metrics
        .write
        .mode("overwrite")
        .parquet("s3a://data-lake-gold/customer_lifetime_metrics/")
    )

    # ===== GOLD 3: High-risk transactions =====
    print("\n" + "=" * 60)
    print("GOLD 3: high_risk_transactions")
    print("=" * 60)

    high_risk = (
        silver
        .filter(col("risk_score") > 60)
        .select(
            "transaction_id", "posted_at", "amount", "currency",
            "transaction_type", "status",
            "customer_id", "customer_name", "country_code", "risk_score",
            "account_id", "account_type",
            "year", "month", "day",
        )
    )
    print(f"High-risk transactions: {high_risk.count():,}")
    high_risk.show(5, truncate=False)

    (
        high_risk
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet("s3a://data-lake-gold/high_risk_transactions/")
    )

    print("\n" + "=" * 60)
    print("ALL GOLD TABLES WRITTEN")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
