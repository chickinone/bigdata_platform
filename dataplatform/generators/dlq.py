"""Cấu hình dead-letter queue cho sink connector + bản kê topic DLQ.

Chính sách: **mọi sink đều bật DLQ**. Không có trường bật/tắt trong contract, vì
"im lặng làm mất bản ghi" không phải một lựa chọn hợp lệ trong hệ thống ngân hàng.
Đây là chính sách nền tảng, không phải quyết định của từng dataset.
"""
from __future__ import annotations

from ..registry import Dataset
from . import es_sink, s3_sink

INVENTORY_PATH = "dlq-processor/dlq_topics.json"


def dlq_topic(connector_name: str) -> str:
    return f"dlq.{connector_name}"


def dlq_config(connector_name: str, replication_factor: str) -> dict:
    """Khối config bật DLQ, dùng chung cho mọi loại sink.

    `replication_factor` truyền vào (không phải hằng số ở đây) vì RF là thuộc tính
    của CLUSTER, sống trong `connections/kafka.yaml`. Trước đây nó bị khai hai lần —
    ở đây và ở topic_manifest — nên topic DLQ và topic dữ liệu có thể lệch RF khi lên
    multi-broker.
    """
    return {
        # all: chuyển bản ghi lỗi sang DLQ thay vì để task chết.
        # Mặc định là `none` -> một message hỏng làm đứng toàn bộ connector.
        # Đánh đổi: `all` chỉ an toàn khi có DLQ + có người nhìn. Cả hai điều
        # kiện đó nay đã có, nên bật được.
        "errors.tolerance": "all",
        "errors.deadletterqueue.topic.name": dlq_topic(connector_name),
        "errors.deadletterqueue.topic.replication.factor": replication_factor,
        # Bắt buộc. Thiếu dòng này thì header __connect.errors.* không được ghi,
        # dlq-processor không đọc được nguyên nhân lỗi, và mọi lỗi rơi vào nhóm
        # UNKNOWN -> phân loại thành vô dụng.
        "errors.deadletterqueue.context.headers.enable": "true",
        # Ghi lỗi ra log Connect để debug nhanh.
        "errors.log.enable": "true",
        # Cố ý không bật errors.log.include.messages: nó in nội dung bản ghi ra
        # log, mà customers chứa full_name/email/phone (PII). Log không phải chỗ
        # cho PII. Nội dung message vẫn còn nguyên trong topic DLQ nếu cần điều tra.
        "errors.log.include.messages": "false",
    }


def connectors(datasets: list[Dataset]) -> list[dict]:
    """Kê mọi sink connector có DLQ, suy từ chính registry.

    Đây là thứ xoá sổ metadata sprawl #12: trước đây danh sách 6 topic DLQ được
    HARDCODE trong dlq_processor.py, tách rời khỏi cấu hình connector. Thêm một
    connector mà quên thêm vào list Python thì lỗi của nó rơi vào hư không.
    """
    out: list[dict] = []

    # Luật "dataset nào có sink X" thuộc về generator của sink X. Gọi hàm của nó thay
    # vì chép biểu thức lọc — nếu chép, nới luật ở generator mà quên ở đây thì
    # `original_topics` thiếu topic và dlq-processor mất khả năng truy nguyên nguồn lỗi
    # (đúng chế độ hỏng của sprawl #12 mà file này được viết để đóng).
    for ds in es_sink.members(datasets):
        name = es_sink.connector_name(ds)
        out.append(
            {
                "connector": name,
                "dlq_topic": dlq_topic(name),
                "original_topics": [ds.topic],
            }
        )

    s3_members = s3_sink.members(datasets)
    if s3_members:
        out.append(
            {
                "connector": s3_sink.CONNECTOR_NAME,
                "dlq_topic": dlq_topic(s3_sink.CONNECTOR_NAME),
                "original_topics": sorted(d.topic for d in s3_members),
            }
        )

    return sorted(out, key=lambda c: c["connector"])


def targets(datasets: list[Dataset]) -> dict[str, dict]:
    """Bản kê để dlq-processor đọc lúc khởi động, thay cho list hardcode."""
    return {
        INVENTORY_PATH: {
            "_comment": (
                "FILE SINH TỰ ĐỘNG - đừng sửa tay. "
                "Nguồn: metadata/datasets/*.yaml. "
                "Sinh lại: python -m dataplatform.cli write"
            ),
            "connectors": connectors(datasets),
        }
    }
