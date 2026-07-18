"""Sinh bản kê (manifest) topic Kafka từ registry — diệt khoảng trống production #8.

VẤN ĐỀ. Hôm nay Kafka bật `auto.create.topics.enable=true`: hễ ai đó produce/consume
một topic chưa tồn tại, broker LẲNG LẶNG tạo nó với partition/retention/RF mặc định.
Tiện lúc dựng lab, nhưng ở production đây là một lỗ hổng:
  - gõ sai tên topic -> tạo ra topic RÁC thay vì báo lỗi;
  - không ai kiểm soát được số partition (giới hạn song song) hay retention;
  - "có những topic nào" không có nguồn sự thật — phải đi hỏi broker đang chạy.

MỤC TIÊU. Khai MỌI topic MỘT LẦN trong registry, sinh ra:
  (a) kafka/topics.json      — manifest khai báo, máy đọc được, diff được;
  (b) kafka/create-topics.sh — script tạo topic idempotent (`--if-not-exists`).
Có hai thứ này rồi thì mới TẮT được `auto.create.topics` một cách an toàn (bước cuối,
sau khi đối chiếu với Kafka thật — xem ADR-0020).

BA NGUỒN TOPIC, và đây là chỗ tinh tế nhất của file:

  1. DATASET  — mọi dataset có `source.topic`. Suy thẳng từ registry. Đây là phần
     "metadata-driven" thật: thêm một dataset = thêm một topic, không đụng file này.

  2. DLQ      — mỗi sink connector có một topic `dlq.<connector>`. KHÔNG tự liệt kê
     lại ở đây; tái dùng đúng danh sách mà generators/dlq.py đã tính, nếu không ta
     lại đẻ ra chính thứ sprawl đang diệt (hai nơi cùng khai danh sách DLQ, lệch nhau).

  3. HẠ TẦNG  — `dlq.events` (đầu ra của dlq-processor) và `_connect_{configs,offsets,
     status}` (topic nội bộ của Kafka Connect). Chúng KHÔNG phải dataset, nên được
     khai TƯỜNG MINH ở đây dưới dạng hằng số có giải thích. Ranh giới quan trọng:
     phần (1)(2) suy ra được, phần (3) phải khai tay — và ta nói rõ điều đó thay vì
     giấu, để người đọc biết chỗ nào là "sự thật suy diễn" chỗ nào là "sự thật khai báo".

NGUYÊN TẮC "TÁI TẠO HIỆN TRẠNG TRƯỚC" (strangler-fig). Manifest đầu tiên phải mô tả
đúng những gì auto-create ĐANG tạo ra, để khi tắt auto-create thì KHÔNG có gì đổi.
Vì vậy: partition = mặc định hiện tại, RF = 1 (single node, ADR-0005). Việc tăng
partition cho `transactions` (throughput cao) hay kéo dài retention cho DLQ là thay
đổi CÓ CHỦ Ý về sau, KHÔNG lén nhét vào bước này — nếu nhét, manifest sẽ lệch với
topic thật và mất luôn khả năng đối chiếu.
"""
from __future__ import annotations

import json

from ..registry import Dataset
from . import dlq

# RF=1 vì Kafka single-node (ADR-0005). Khoá ở MỘT chỗ; lên multi-broker thì đây là
# giá trị đổi theo env — cùng lý do và cùng kiểu với DLQ_REPLICATION_FACTOR bên dlq.py.
REPLICATION_FACTOR = 1

# Số partition mặc định cho topic dữ liệu. Bằng 1 để TÁI TẠO đúng thứ auto-create
# đang sinh ra (broker không set num.partitions -> mặc định Kafka = 1). Tăng nó là
# quyết định hiệu năng riêng, có ràng buộc (đổi partition của topic nguồn ảnh hưởng
# key_by của fraud detector) — làm sau, có đối chiếu.
DEFAULT_PARTITIONS = 1

# Topic đầu ra của dlq-processor (dlq_processor.py: EVENTS_TOPIC = "dlq.events").
# Đây là topic PIPELINE nội bộ, không gắn dataset nào — nên khai tay ở đây.
DLQ_EVENTS_TOPIC = "dlq.events"


def _topic(name: str, provenance: str, *, partitions: int = DEFAULT_PARTITIONS,
           compact: bool = False) -> dict:
    """Một dòng manifest. `provenance` trả lời 'topic này từ đâu ra' — để người đọc
    (và catalog về sau) truy được nguồn, không phải đoán.

    `compact`: cleanup.policy=compact giữ BẢN GHI MỚI NHẤT theo key thay vì xoá theo
    thời gian. Bắt buộc cho topic nội bộ của Connect (chúng là store key-value:
    config/offset/status hiện tại của connector, không phải một dòng lịch sử).
    """
    # Chỉ topic COMPACTED mới khai cleanup.policy. Topic `delete` để configs RỖNG,
    # KHÔNG khai `cleanup.policy=delete` — vì đó đã là mặc định broker, và topic thật
    # do auto-create sinh ra KHÔNG có override này (cột Configs trống khi describe).
    # Khai thừa sẽ làm manifest lệch với hiện trạng và mất khả năng đối chiếu sạch.
    # Cùng lý do retention để mặc định broker (tinh chỉnh sau, có chủ ý).
    configs = {"cleanup.policy": "compact"} if compact else {}
    return {
        "name": name,
        "partitions": partitions,
        "replication_factor": REPLICATION_FACTOR,
        "configs": configs,
        "provenance": provenance,
    }


def _entries(datasets: list[Dataset]) -> list[dict]:
    """Gộp cả ba nguồn topic thành một danh sách đã sắp xếp ổn định."""
    entries: list[dict] = []

    # (1) DATASET — suy thẳng từ registry.
    for ds in datasets:
        entries.append(_topic(ds.topic, f"dataset:{ds.urn}"))

    # (2) DLQ — tái dùng danh sách của dlq.py, KHÔNG tự liệt kê lại.
    for conn in dlq.connectors(datasets):
        entries.append(_topic(conn["dlq_topic"], f"dlq:{conn['connector']}"))

    # (3) HẠ TẦNG — khai tay, có giải thích. Đây là các topic KHÔNG gắn dataset nào
    # nhưng vẫn phải tồn tại trước khi dám tắt auto.create.topics. Danh sách này được
    # chốt bằng cách ĐỐI CHIẾU với Kafka thật (ADR-0020), không phải đoán — `_schemas`
    # lọt lưới ở bản đầu và chỉ lộ ra khi describe cluster đang chạy.
    entries.append(_topic(DLQ_EVENTS_TOPIC, "pipeline:dlq-processor"))
    # Topic nội bộ Connect: partition khớp mặc định Connect (offsets=25, status=5,
    # configs=1) để tái tạo đúng hiện trạng; tất cả compacted.
    entries.append(_topic("_connect_configs", "infra:kafka-connect", partitions=1, compact=True))
    entries.append(_topic("_connect_offsets", "infra:kafka-connect", partitions=25, compact=True))
    entries.append(_topic("_connect_status", "infra:kafka-connect", partitions=5, compact=True))
    # Store schema của Confluent Schema Registry: single-partition, compacted (SR bắt
    # buộc như vậy). SR tự tạo, nhưng vẫn phải khai để manifest là bản kê ĐẦY ĐỦ.
    entries.append(_topic("_schemas", "infra:schema-registry", partitions=1, compact=True))
    # LƯU Ý: __consumer_offsets KHÔNG nằm đây. Nó do chính broker Kafka quản, tạo bất
    # kể auto.create.topics, không phải thứ control plane khai hay xoá được.

    # Sắp theo tên để output ổn định giữa các lần chạy — điều kiện để `check` có nghĩa.
    return sorted(entries, key=lambda t: t["name"])


# Thứ tự nhóm khi in script — để một loại topic nằm liền khối, tiêu đề không lặp.
_KIND_ORDER = {"dataset": 0, "dlq": 1, "pipeline": 2, "infra": 3}
_KIND_HEADING = {
    "dataset": "# --- Topic dữ liệu (sinh từ dataset contract) ---",
    "dlq": "# --- Topic dead-letter (một cái mỗi sink connector) ---",
    "pipeline": "# --- Topic pipeline nội bộ ---",
    "infra": "# --- Topic nội bộ Kafka Connect (compacted) ---",
}


def _kind(entry: dict) -> str:
    return entry["provenance"].split(":", 1)[0]


_JSON_COMMENT = (
    "FILE SINH TỰ ĐỘNG - đừng sửa tay. "
    "Nguồn: metadata/datasets/*.yaml + generators/dlq.py + hằng số hạ tầng trong "
    "generators/topic_manifest.py. Sinh lại: python -m dataplatform.cli write"
)


def render_manifest(datasets: list[Dataset]) -> str:
    """Manifest JSON — trả về CHUỖI (không phải dict) để `check` so BYTE-EXACT.

    Vì sao byte-exact chứ không so ngữ nghĩa như connector JSON: file này DO CONTROL
    PLANE SỞ HỮU HOÀN TOÀN, không công cụ ngoài nào format lại nó. Byte-match vừa
    chặt hơn vừa tránh bẫy: bộ so ngữ nghĩa của cli.py được viết riêng cho hình dạng
    {name, config} của connector, đưa manifest {topics:[...]} vào đó sẽ luôn báo KHỚP
    một cách sai. Trả chuỗi là đi thẳng nhánh so nguyên văn (giống DDL SQL).
    """
    payload = {"_comment": _JSON_COMMENT, "topics": _entries(datasets)}
    return json.dumps(payload, indent=2, ensure_ascii=False) + "\n"


def render_script(datasets: list[Dataset]) -> str:
    """Script tạo topic idempotent, chạy được trong image confluent cp-kafka.

    `--if-not-exists`: topic đã có thì bỏ qua, không lỗi -> chạy lại bao nhiêu lần
    cũng an toàn. Đây là dạng THỰC THI của manifest; cả hai sinh từ cùng _entries()
    nên không thể lệch nhau.
    """
    lines = [
        "#!/usr/bin/env bash",
        "# " + _JSON_COMMENT,
        "#",
        "# Tạo mọi topic mà hệ thống cần, idempotent. Chạy TRƯỚC khi tắt",
        "# auto.create.topics (xem ADR-0020). Vd:",
        "#   docker exec bigdata-kafka bash /opt/bitnami/kafka/create-topics.sh",
        "set -euo pipefail",
        "",
        'BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"',
        "",
    ]

    # Gom theo nhóm (dataset -> dlq -> pipeline -> infra), trong nhóm sắp theo tên.
    # Manifest JSON vẫn sắp thuần theo tên; script gom nhóm để người chạy dễ đọc.
    ordered = sorted(_entries(datasets), key=lambda t: (_KIND_ORDER[_kind(t)], t["name"]))

    last_kind = None
    for t in ordered:
        kind = _kind(t)
        if kind != last_kind:
            lines.append(_KIND_HEADING[kind])
            last_kind = kind

        cfg = " ".join(f"--config {k}={v}" for k, v in t["configs"].items())
        lines.append(
            f'kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists '
            f'--topic {t["name"]} '
            f'--partitions {t["partitions"]} --replication-factor {t["replication_factor"]} '
            f'{cfg}  # {t["provenance"]}'
        )
    lines.append("")
    lines.append('echo "Đã đảm bảo tồn tại $(grep -c \'^kafka-topics\' "$0") topic."')
    lines.append("")
    return "\n".join(lines)


def targets(datasets: list[Dataset]) -> dict[str, str]:
    return {
        "kafka/topics.json": render_manifest(datasets),
        "kafka/create-topics.sh": render_script(datasets),
    }
