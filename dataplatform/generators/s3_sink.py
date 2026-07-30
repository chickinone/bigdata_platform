from __future__ import annotations

from ..registry import Dataset, endpoint

CONNECTOR_NAME = "s3-sink-cdc"
CONNECTOR_CLASS = "io.confluent.connect.s3.S3SinkConnector"


def members(datasets: list[Dataset]) -> list[Dataset]:
    """Dataset nào đi vào Bronze.

    Chỉ CDC mới vào Bronze: Bronze là bản sao trung thực của dữ liệu nguồn.
    fraud-alerts là dữ liệu phái sinh do Flink đẻ ra — tính lại được từ nguồn,
    nên không cần lưu trữ lâu dài ở lake.

    Public (không `_`) vì `dlq.py` phải dùng CHÍNH luật này để kê `original_topics`.
    Trước đây dlq.py chép lại biểu thức lọc — hai bản copy của một luật, nới luật ở
    đây mà quên chỗ kia thì DLQ mất khả năng truy nguyên topic gốc.
    """
    return [d for d in datasets if d.is_cdc and d.sink_enabled("s3_bronze")]


def render(datasets: list[Dataset], conns: dict[str, dict]) -> dict:
    selected = members(datasets)
    sr = endpoint(conns, "schema_registry", "connect_url")
    config = {
        "connector.class": CONNECTOR_CLASS,
        "tasks.max": "2",
        # Gộp topic của mọi dataset bật s3_bronze. Trước đây danh sách này được
        # chép tay - quên một topic là mất dữ liệu lake mà không có lỗi nào
        # (metadata sprawl #4).
        "topics": ",".join(d.topic for d in selected),

        # Endpoint object store đọc từ connection s3_minio, không hardcode.
        "s3.bucket.name": endpoint(conns, "s3_minio", "bucket_bronze"),
        "s3.region": endpoint(conns, "s3_minio", "region"),
        "store.url": endpoint(conns, "s3_minio", "store_url"),
        "aws.access.key.id": endpoint(conns, "s3_minio", "access_key"),
        "aws.secret.access.key": endpoint(conns, "s3_minio", "secret_key"),
        "s3.path.style.access.enabled": endpoint(conns, "s3_minio", "path_style_access"),

        "storage.class": "io.confluent.connect.s3.storage.S3Storage",
        "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
        "parquet.codec": "snappy",

        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": sr,
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": sr,

        # Ghi file khi đủ 1000 record hoặc sau 5 phút - cái nào tới trước.
        "flush.size": "1000",
        "rotate.interval.ms": "300000",
        "rotate.schedule.interval.ms": "600000",

        "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
        "partition.duration.ms": "3600000",
        "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
        # Record = dùng timestamp TRONG message, không phải lúc ghi. Nhờ vậy
        # replay cho ra cùng layout partition.
        "timestamp.extractor": "Record",
        "locale": "en-US",
        "timezone": "UTC",

        "schema.compatibility": "NONE",

        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        # rewrite: DELETE thành row có cờ __deleted thay vì biến mất.
        "transforms.unwrap.delete.handling.mode": "rewrite",
    }

    from .dlq import dlq_config

    config.update(dlq_config(CONNECTOR_NAME, endpoint(conns, "kafka", "replication_factor")))

    return {"name": CONNECTOR_NAME, "config": config}


def targets(datasets: list[Dataset], conns: dict[str, dict]) -> dict[str, dict]:
    if not members(datasets):
        return {}
    return {f"kafka-connect/s3-sinks/{CONNECTOR_NAME}.json": render(datasets, conns)}
