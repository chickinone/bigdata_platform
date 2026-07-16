"""Sinh config S3 sink connector (Bronze) từ dataset contract.

Khác ES sink ở một điểm quan trọng về mô hình:

    ES sink   : MỘT dataset  -> MỘT connector   (quan hệ 1-1)
    S3 sink   : NHIỀU dataset -> MỘT connector   (quan hệ N-1, gộp `topics`)

Đây là generator "tổng hợp" đầu tiên — nó phải nhìn TOÀN BỘ registry chứ không
chỉ một contract. Cũng chính là hình dạng mà Debezium `table.include.list` sẽ cần.
"""
from __future__ import annotations

from ..registry import Dataset

CONNECTOR_NAME = "s3-sink-cdc"
CONNECTOR_CLASS = "io.confluent.connect.s3.S3SinkConnector"


def _members(datasets: list[Dataset]) -> list[Dataset]:
    """Dataset nào đi vào Bronze.

    Chỉ CDC mới vào Bronze: Bronze là bản sao trung thực của DỮ LIỆU NGUỒN.
    fraud-alerts là dữ liệu phái sinh do Flink đẻ ra — tính lại được từ nguồn,
    nên không cần lưu trữ lâu dài ở lake.
    """
    return [d for d in datasets if d.is_cdc and d.sink_enabled("s3_bronze")]


def render(datasets: list[Dataset]) -> dict:
    members = _members(datasets)
    config = {
        "connector.class": CONNECTOR_CLASS,
        "tasks.max": "2",
        # Gộp topic của mọi dataset bật s3_bronze. Trước đây danh sách này được
        # chép tay - quên một topic là mất dữ liệu lake mà không có lỗi nào
        # (metadata sprawl #4).
        "topics": ",".join(d.topic for d in members),

        "s3.bucket.name": "${env:S3_BUCKET_BRONZE}",
        "s3.region": "${env:S3_REGION}",
        "store.url": "${env:S3_ENDPOINT}",
        "aws.access.key.id": "${env:S3_ACCESS_KEY}",
        "aws.secret.access.key": "${env:S3_SECRET_KEY}",
        "s3.path.style.access.enabled": "${env:S3_PATH_STYLE_ACCESS}",

        "storage.class": "io.confluent.connect.s3.storage.S3Storage",
        "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
        "parquet.codec": "snappy",

        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": "${env:SCHEMA_REGISTRY_URL}",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": "${env:SCHEMA_REGISTRY_URL}",

        # Ghi file khi đủ 1000 record HOẶC sau 5 phút - cái nào tới trước.
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

    config.update(dlq_config(CONNECTOR_NAME))

    return {"name": CONNECTOR_NAME, "config": config}


def targets(datasets: list[Dataset]) -> dict[str, dict]:
    if not _members(datasets):
        return {}
    return {f"kafka-connect/s3-sinks/{CONNECTOR_NAME}.json": render(datasets)}
