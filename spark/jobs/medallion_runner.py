"""Runner medallion tổng quát — THỰC THI batch job plan sinh từ metadata.

Data plane của Pha 5: KHÔNG chứa logic transform nào. Job plan (inputs + SQL +
output) sinh trên host từ batch pipeline spec (`metadata/pipelines/batch/*.yaml`,
ADR-0024). Runner này chỉ: đọc input thành view -> chạy SQL -> ghi theo output.

Mô hình dbt: transform là SQL (ETL medallion khác khuôn), còn schema/path/format/
partition của output khai trong spec. Thay enrich_transactions.py + build_gold_layer.py.
"""
import json
import os

from pyspark.sql import SparkSession


def build_spark(name: str, iceberg: bool) -> SparkSession:
    b = (
        SparkSession.builder
        .appName(name)
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("S3_ACCESS_KEY", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_SECRET_KEY", ""))
        .config("spark.hadoop.fs.s3a.path.style.access", os.getenv("S3_PATH_STYLE_ACCESS", "true"))
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", os.getenv("S3_SSL_ENABLED", "false"))
    )
    if iceberg:
        # Chỉ thêm khi output là iceberg (cần iceberg jar do deployer nạp riêng).
        # Cùng cấu hình đã chứng minh ở silver_to_iceberg.py: catalog REST + HadoopFileIO
        # (S3A battle-tested với MinIO, tránh S3FileIO hang multipart).
        b = (
            b.config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "rest")
            .config("spark.sql.catalog.lakehouse.uri", os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181"))
            .config("spark.sql.catalog.lakehouse.warehouse", os.getenv("ICEBERG_WAREHOUSE", "s3a://data-lake-iceberg/warehouse"))
            .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
            .config("spark.sql.defaultCatalog", "lakehouse")
        )
    return b.getOrCreate()


def main():
    with open(os.environ["JOB_PLAN"], encoding="utf-8") as f:
        plan = json.load(f)

    out = plan["output"]
    is_iceberg = out.get("format") == "iceberg"
    spark = build_spark(plan["name"], iceberg=is_iceberg)
    spark.sparkContext.setLogLevel("WARN")

    # Input: mỗi parquet -> view có tên (SQL tham chiếu tên này).
    for inp in plan["inputs"]:
        spark.read.parquet(inp["path"]).createOrReplaceTempView(inp["view"])
        print(f"  view {inp['view']:<24} <- {inp['path']}")

    # Transform: SQL khai trong spec.
    result = spark.sql(plan["sql"]).cache()
    rows = result.count()

    if is_iceberg:
        # CTAS Iceberg: tạo namespace rồi ghi đè bảng. Iceberg tự quản snapshot/time-travel.
        table = out["table"]
        namespace = table.rsplit(".", 1)[0]
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        result.writeTo(table).using("iceberg").createOrReplace()
        target = table
    else:
        writer = result.write.mode(out["mode"])
        if out.get("partition_by"):
            writer = writer.partitionBy(*out["partition_by"])
        writer.format(out.get("format", "parquet")).save(out["path"])
        target = out["path"]

    print(f"WROTE {plan['name']}: {rows:,} rows -> {target}")
    spark.stop()


if __name__ == "__main__":
    main()
