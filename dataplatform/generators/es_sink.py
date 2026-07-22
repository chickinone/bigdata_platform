"""Sinh config Elasticsearch sink connector từ dataset contract.

Thay cho 5 file JSON viết tay trong kafka-connect/es-sinks/.

Ý chính: mọi thứ trong config đều được SUY RA từ contract, không khai lại.
Ví dụ rõ nhất là `key.ignore` và `write.method` — chúng không phải hai lựa chọn
độc lập mà là hệ quả của một sự thật: "dataset này có khoá chính hay không".

Viết tay thì hai trường đó có thể lệch nhau (key.ignore=false nhưng không có
extractKey → connector chết lúc chạy). Sinh ra thì chúng không thể lệch, vì cùng
đọc một nguồn.
"""
from __future__ import annotations

from ..registry import Dataset, endpoint

CONNECTOR_CLASS = "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector"


def connector_name(ds: Dataset) -> str:
    return f"es-sink-{ds.entity}"


def render(ds: Dataset, conns: dict[str, dict]) -> dict:
    """Dựng config connector cho một dataset.

    Rẽ nhánh theo `source.type` chứ không theo tên dataset — nhờ vậy thêm một
    stream mới kiểu app_json sẽ tự đi đúng nhánh, không cần sửa generator.

    Endpoint (ES url + schema registry) đọc từ connection registry, không hardcode.
    """
    config = {
        "connector.class": CONNECTOR_CLASS,
        "tasks.max": "1",
        "topics": ds.topic,
        "connection.url": endpoint(conns, "elasticsearch_serving", "connect_url"),
    }

    if ds.is_cdc:
        config.update(_cdc_converters(endpoint(conns, "schema_registry", "connect_url")))
    else:
        config.update(_json_converters())

    config.update(
        {
            "type.name": "_doc",
            # Đây là chỗ "một sự thật, nhiều trường": có PK -> upsert được;
            # không PK -> ES tự sinh _id, chỉ append được.
            "key.ignore": "false" if ds.primary_key else "true",
            # schema.ignore=true: ES tự suy mapping. Giữ đúng hiện trạng để bước
            # đối chiếu sạch. Đây là hạn chế đã biết (ADR-0011) - sẽ xử lý riêng
            # bằng index template, không lẫn vào thay đổi này.
            "schema.ignore": "true",
            "write.method": "upsert" if ds.primary_key else "insert",
            "behavior.on.null.values": "delete" if ds.primary_key else "ignore",
            "behavior.on.malformed.documents": "warn",
        }
    )

    if ds.is_cdc:
        config.update(_cdc_transforms(ds))

    # DLQ cho mọi sink - chính sách nền tảng, xem generators/dlq.py.
    # Import tại chỗ để tránh vòng lặp import (dlq.py cần connector_name từ đây).
    from .dlq import dlq_config

    config.update(dlq_config(connector_name(ds)))

    return {"name": connector_name(ds), "config": config}


def _cdc_converters(schema_registry_url: str) -> dict:
    """CDC đi qua Debezium + Schema Registry nên phải dùng Avro converter."""
    return {
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": schema_registry_url,
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": schema_registry_url,
    }


def _json_converters() -> dict:
    """Stream do app tự ghi (vd Flink dùng SimpleStringSchema) là JSON trần:
    không Avro, không Schema Registry, key là chuỗi thô.
    """
    return {
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
    }


def _cdc_transforms(ds: Dataset) -> dict:
    """Hai transform, chỉ CDC mới cần:

    - unwrap: bóc envelope {op,ts_ms,before,after} để chỉ giữ `after`. Không có
      nó, ES sẽ lưu cả cục envelope thay vì bản ghi nghiệp vụ.
    - extractKey: lấy PK từ key của message làm `_id` của document. Đây là thứ
      biến "append log" thành "bảng trạng thái hiện tại".
    """
    return {
        "transforms": "unwrap,extractKey",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        "transforms.unwrap.delete.handling.mode": "none",
        "transforms.extractKey.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
        # PK đọc thẳng từ contract. Đây chính là metadata sprawl #5 bị xoá sổ:
        # trước đây PK bị chép tay vào JSON, tách rời khỏi định nghĩa bảng.
        "transforms.extractKey.field": ds.primary_key,
    }


def targets(datasets: list[Dataset], conns: dict[str, dict]) -> dict[str, dict]:
    """Trả về {đường_dẫn_tương_đối: nội_dung} cho mọi dataset bật ES sink."""
    out = {}
    for ds in datasets:
        if not ds.sink_enabled("elasticsearch"):
            continue
        out[f"kafka-connect/es-sinks/{connector_name(ds)}.json"] = render(ds, conns)
    return out
