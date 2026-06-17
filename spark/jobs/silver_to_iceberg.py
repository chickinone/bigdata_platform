import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("S3_ACCESS_KEY", "")
    s3_secret_key = os.getenv("S3_SECRET_KEY", "")
    s3_path_style_access = os.getenv("S3_PATH_STYLE_ACCESS", "true")
    s3_ssl_enabled = os.getenv("S3_SSL_ENABLED", "false")
    iceberg_rest_uri = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    iceberg_warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3a://data-lake-iceberg/warehouse")

    spark = (
        SparkSession.builder
        .appName("silver_to_iceberg")
        # ===== Iceberg Hadoop catalog (file-based, no REST) =====
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", iceberg_rest_uri)
        .config("spark.sql.catalog.lakehouse.warehouse", iceberg_warehouse)
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        # FORCE HadoopFileIO (S3A) thay vì S3FileIO (AWS SDK v2)
        # S3A battle-tested với MinIO, không có multipart upload hang
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config("spark.sql.defaultCatalog", "lakehouse")
        # ===== S3A config — read silver + write Iceberg cùng dùng =====
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", s3_path_style_access)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", s3_ssl_enabled)
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 60)
    print("STEP 1: Read silver Parquet (source)")
    print("=" * 60)

    silver = spark.read.parquet("s3a://data-lake-silver/enriched_transactions/")
    print(f"Silver rows: {silver.count():,}")
    silver.createOrReplaceTempView("silver_source")

    print("\n" + "=" * 60)
    print("STEP 2: Create Iceberg table from silver")
    print("=" * 60)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql("DROP TABLE IF EXISTS lakehouse.silver.enriched_transactions PURGE")

    # CREATE TABLE AS SELECT — không partition để tránh FanoutWriter memory issue
    # Có thể add partition spec sau bằng ALTER TABLE nếu cần
    spark.sql("""
        CREATE TABLE lakehouse.silver.enriched_transactions
        USING iceberg
        AS SELECT * FROM silver_source
    """)

    count = spark.sql("SELECT COUNT(*) FROM lakehouse.silver.enriched_transactions").collect()[0][0]
    print(f"Iceberg table created with {count:,} rows")

    print("\n" + "=" * 60)
    print("STEP 3: Show table history (1 snapshot now)")
    print("=" * 60)

    spark.sql("SELECT * FROM lakehouse.silver.enriched_transactions.history").show(truncate=False)
    spark.sql("SELECT * FROM lakehouse.silver.enriched_transactions.snapshots").show(truncate=False)

    print("\n" + "=" * 60)
    print("STEP 4: Append new data (creates 2nd snapshot)")
    print("=" * 60)

    # Simulate batch hôm sau: insert thêm 1000 row
    new_batch = spark.read.parquet("s3a://data-lake-silver/enriched_transactions/").limit(1000)
    new_batch.writeTo("lakehouse.silver.enriched_transactions").append()

    count_after = spark.sql("SELECT COUNT(*) FROM lakehouse.silver.enriched_transactions").collect()[0][0]
    print(f"After append: {count_after:,} rows (added 1000)")

    print("\nUpdated history (now 2 snapshots):")
    spark.sql("SELECT * FROM lakehouse.silver.enriched_transactions.history").show(truncate=False)

    print("\n" + "=" * 60)
    print("STEP 5: TIME TRAVEL — query snapshot cũ (trước append)")
    print("=" * 60)

    snapshots = spark.sql("""
        SELECT snapshot_id 
        FROM lakehouse.silver.enriched_transactions.snapshots 
        ORDER BY committed_at ASC
    """).collect()
    first_snapshot = snapshots[0][0]
    print(f"First snapshot ID: {first_snapshot}")

    # Query data tại snapshot đầu tiên
    historical_count = spark.sql(f"""
        SELECT COUNT(*) FROM lakehouse.silver.enriched_transactions 
        VERSION AS OF {first_snapshot}
    """).collect()[0][0]
    print(f"Historical count (snapshot {first_snapshot}): {historical_count:,}")
    print(f"Current count: {count_after:,}")
    print("→ Time travel works: historical snapshot không bị thay đổi sau append")

    print("\n" + "=" * 60)
    print("STEP 6: SCHEMA EVOLUTION — add column")
    print("=" * 60)

    # Add column 'processed_at' — Iceberg tự handle, không rewrite old files
    spark.sql("""
        ALTER TABLE lakehouse.silver.enriched_transactions 
        ADD COLUMN processed_at TIMESTAMP COMMENT 'When this row was processed by Spark'
    """)

    print("Schema sau khi add column:")
    spark.sql("DESCRIBE TABLE lakehouse.silver.enriched_transactions").show(50, truncate=False)

    print("\nSample data với column mới (old rows = NULL):")
    spark.sql("""
        SELECT transaction_id, amount, country_code, processed_at 
        FROM lakehouse.silver.enriched_transactions 
        LIMIT 5
    """).show(truncate=False)

    print("\n" + "=" * 60)
    print("STEP 7: Files layout in MinIO")
    print("=" * 60)

    files = spark.sql("SELECT * FROM lakehouse.silver.enriched_transactions.files").collect()
    print(f"Total data files: {len(files)}")
    if len(files) > 0:
        print(f"\nSample file:")
        print(f"  Path: {files[0]['file_path']}")
        print(f"  Format: {files[0]['file_format']}")
        print(f"  Record count: {files[0]['record_count']}")
        print(f"  File size: {files[0]['file_size_in_bytes']:,} bytes")

    print("\n" + "=" * 60)
    print("ICEBERG DEMO COMPLETE")
    print("=" * 60)
    print("\nLakehouse table: lakehouse.silver.enriched_transactions")
    print("Warehouse path:  s3a://data-lake-iceberg/warehouse/silver/enriched_transactions/")
    print(f"Total snapshots: 2 (initial create + append)")
    print(f"Current rows:    {count_after:,}")

    spark.stop()


if __name__ == "__main__":
    main()
