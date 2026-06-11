
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, year, month, dayofmonth, row_number


def main():
    spark = (
        SparkSession.builder
        .appName("enrich_transactions")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 60)
    print("READING BRONZE LAYER (raw CDC events)")
    print("=" * 60)

    customers = spark.read.parquet("s3a://data-lake-bronze/topics/bankdb.public.customers/")
    accounts = spark.read.parquet("s3a://data-lake-bronze/topics/bankdb.public.accounts/")
    transactions = spark.read.parquet("s3a://data-lake-bronze/topics/bankdb.public.transactions/")

    print(f"Customers (raw events):    {customers.count():>10,}")
    print(f"Accounts (raw events):     {accounts.count():>10,}")
    print(f"Transactions (raw events): {transactions.count():>10,}")

    print("\n" + "=" * 60)
    print("DEDUP TO CURRENT STATE")
    print("=" * 60)

    # Dedup accounts: giữ row có updated_at MỚI NHẤT per account_id
    accounts_current = (
        accounts
        .withColumn("rn", row_number().over(
            Window.partitionBy("account_id").orderBy(col("updated_at").desc())
        ))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    print(f"Accounts (current state):  {accounts_current.count():>10,}")

    # Customers cũng dedup (defensive — sẽ giữ nguyên 100)
    customers_current = (
        customers
        .withColumn("rn", row_number().over(
            Window.partitionBy("customer_id").orderBy(col("updated_at").desc())
        ))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    print(f"Customers (current state): {customers_current.count():>10,}")

    print("\n" + "=" * 60)
    print("ENRICHMENT JOIN")
    print("=" * 60)

    enriched = (
        transactions.alias("t")
        .join(accounts_current.alias("a"), col("t.account_id") == col("a.account_id"))
        .join(customers_current.alias("c"), col("a.customer_id") == col("c.customer_id"))
        .select(
            col("t.transaction_id"),
            col("t.transaction_type"),
            col("t.amount"),
            col("t.currency"),
            col("t.status"),
            col("t.posted_at"),
            col("a.account_id"),
            col("a.account_type"),
            col("a.account_number"),
            col("c.customer_id"),
            col("c.full_name").alias("customer_name"),
            col("c.country_code"),
            col("c.kyc_status"),
            col("c.risk_score"),
        )
        .withColumn("year", year(col("posted_at")))
        .withColumn("month", month(col("posted_at")))
        .withColumn("day", dayofmonth(col("posted_at")))
    )

    enriched_count = enriched.count()
    print(f"Enriched transactions: {enriched_count:,}")
    enriched.printSchema()
    enriched.show(5, truncate=False)

    print("\n" + "=" * 60)
    print("WRITING SILVER LAYER")
    print("=" * 60)

    (
        enriched
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet("s3a://data-lake-silver/enriched_transactions/")
    )

    print("Silver layer written: s3a://raw-data-lake/silver/enriched_transactions/")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()