"""Sinh DDL ClickHouse cho tầng serving metric.

Đóng sprawl #8/#9 — chỗ gây ra chế độ hỏng khó chịu nhất hệ thống.

Mỗi metric cần đúng bộ 3 đối tượng, và schema của cả 3 phải khớp tuyệt đối:

    metrics.<m>          bảng đích   (ReplacingMergeTree)  <- Grafana đọc
    metrics.<m>_kafka    bảng đệm    (Kafka engine)        <- kéo từ topic
    metrics.<m>_mv       MV          (INSERT ... SELECT)   <- nối 2 cái trên

Viết tay = 4 metric × 3 = 12 khối schema phải khớp nhau bằng tay, cộng 4 sink DDL
bên Flink. Lệch một cột thì MV **bỏ dữ liệu mà không báo lỗi** — dashboard vẫn
xanh, chỉ là rỗng. Sinh ra thì cả 3 cùng đọc `columns` của một contract, nên
không thể lệch.

Một điểm tinh tế: cùng một cột logic được render khác nhau tuỳ đối tượng —
`tx_type` là `LowCardinality(String)` ở bảng đích nhưng `String` ở bảng Kafka.
Đó là lý do "một spec, nhiều cách render đúng" chứ không phải "copy 3 lần".
"""
from __future__ import annotations

from ..registry import Dataset

# Ánh xạ kiểu logic -> kiểu ClickHouse.
# Đây là toàn bộ tri thức về ClickHouse của control plane, khoá ở một chỗ.
_BASE_TYPES = {
    "timestamp": "DateTime64(3)",
    "string": "String",
    "boolean": "UInt8",
    "double": "Float64",
    "date": "Date",
}


def _ch_type(col: dict, *, low_cardinality_ok: bool, overrides: dict) -> str:
    """Dịch một cột logic sang kiểu ClickHouse.

    `low_cardinality_ok` là mấu chốt: bảng đích dùng LowCardinality(String) để
    nén, nhưng bảng Kafka phải dùng String thô (Kafka engine parse JSON, không
    hưởng lợi từ LowCardinality). Cùng cột, hai cách render — đúng cả hai.
    """
    name = col["name"]
    if name in overrides:
        return overrides[name]  # cửa thoát hiểm, vd rank_num -> UInt8

    logical = col["type"]

    if logical.startswith("decimal("):
        precision, scale = logical[len("decimal("):-1].split(",")
        # ClickHouse in ra có khoảng trắng sau dấu phẩy: Decimal(19, 4)
        return f"Decimal({precision.strip()}, {scale.strip()})"

    if logical in ("long", "int"):
        if not col.get("unsigned"):
            return "Int64" if logical == "long" else "Int32"
        return "UInt64" if logical == "long" else "UInt32"

    if logical == "string" and col.get("low_cardinality") and low_cardinality_ok:
        return "LowCardinality(String)"

    return _BASE_TYPES[logical]


def ch_datasets(datasets: list[Dataset]) -> list[Dataset]:
    members = [d for d in datasets if d.sink_enabled("clickhouse")]
    return sorted(members, key=lambda d: d.raw["sinks"]["clickhouse"]["table"])


def _spec(ds: Dataset) -> dict:
    return ds.raw["sinks"]["clickhouse"]


def _col_block(ds: Dataset, *, low_cardinality_ok: bool, indent: str = "    ") -> str:
    """Khối danh sách cột — dùng chung cho cả bảng đích lẫn bảng Kafka.

    Chính hàm này là thứ đảm bảo 2 bảng không lệch cột: chúng gọi cùng một hàm
    trên cùng một `columns`.
    """
    spec = _spec(ds)
    overrides = spec.get("column_types", {})
    cols = ds.columns()
    width = max(len(c["name"]) for c in cols)
    lines = [
        f'{indent}{c["name"]:<{width}}  '
        f'{_ch_type(c, low_cardinality_ok=low_cardinality_ok, overrides=overrides)}'
        for c in cols
    ]
    return ",\n".join(lines)


def render_target_table(ds: Dataset) -> str:
    """Bảng đích — nơi lưu thật, Grafana đọc."""
    spec = _spec(ds)
    db, table = spec["database"], spec["table"]

    cols = _col_block(ds, low_cardinality_ok=True)

    # version_column chỉ có ở bảng đích, không có ở Kafka/MV — nó thuộc tầng
    # serving (cột phiên bản của ReplacingMergeTree), không phải schema metric.
    version = spec.get("version_column")
    if version:
        cols += f",\n    {version:<13} DateTime DEFAULT now()"

    engine = spec["engine"]
    engine_clause = f"{engine}({version})" if version and engine == "ReplacingMergeTree" else engine

    sql = f"CREATE TABLE IF NOT EXISTS {db}.{table} (\n{cols}\n)\nENGINE = {engine_clause}"
    if spec.get("partition_by"):
        sql += f'\nPARTITION BY {spec["partition_by"]}'
    sql += f'\nORDER BY ({", ".join(spec["order_by"])})'
    if spec.get("ttl_column") and spec.get("ttl_interval"):
        sql += f'\nTTL toDateTime({spec["ttl_column"]}) + INTERVAL {spec["ttl_interval"]}'
    return sql + ";"


def render_kafka_table(ds: Dataset) -> str:
    """Bảng đệm Kafka engine — đọc một lần là mất.

    Lưu ý: không có version_column, và String thay vì LowCardinality.
    """
    spec = _spec(ds)
    db, table = spec["database"], spec["table"]
    cols = _col_block(ds, low_cardinality_ok=False)

    settings = [
        "kafka_broker_list = 'kafka:9092'",
        f"kafka_topic_list = '{ds.topic}'",
        f"kafka_group_name = '{spec['kafka_group']}'",
        "kafka_format = 'JSONEachRow'",
        "kafka_num_consumers = 1",
    ]
    if spec.get("kafka_max_block_size"):
        settings.append(f'kafka_max_block_size = {spec["kafka_max_block_size"]}')

    body = ",\n    ".join(settings)
    return (
        f"CREATE TABLE IF NOT EXISTS {db}.{table}_kafka (\n{cols}\n) ENGINE = Kafka\n"
        f"SETTINGS\n    {body};"
    )


def render_mv(ds: Dataset) -> str:
    """Materialized View — chỉ là trigger INSERT ... SELECT.

    Danh sách cột SELECT phải khớp cột bảng đích, trừ version_column (nó có
    DEFAULT now()). Đây chính là chỗ dễ lệch nhất khi viết tay.
    """
    spec = _spec(ds)
    db, table = spec["database"], spec["table"]
    names = ", ".join(c["name"] for c in ds.columns())
    return (
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.{table}_mv\n"
        f"TO {db}.{table} AS\nSELECT\n    {names}\nFROM {db}.{table}_kafka;"
    )


_HEADER = """\
-- =====================================================================
-- FILE SINH TỰ ĐỘNG — đừng sửa tay.
--   Nguồn:    metadata/datasets/metrics/*.yaml
--   Sinh lại: python -m dataplatform.cli write
--
{purpose}
-- =====================================================================
"""


def render_schema_file(datasets: list[Dataset]) -> str:
    purpose = (
        "-- Bảng đích cho mỗi metric (nơi lưu thật, Grafana đọc).\n"
        "-- Cột sinh từ `columns` của contract, nên luôn khớp bảng Kafka + MV\n"
        "-- ở 02_kafka_consumers.sql (diệt sprawl #8/#9)."
    )
    parts = [_HEADER.format(purpose=purpose)]
    for ds in ch_datasets(datasets):
        parts.append(f"\n-- {ds.urn}\n{render_target_table(ds)}\n")
    return "".join(parts)


def render_consumers_file(datasets: list[Dataset]) -> str:
    purpose = (
        "-- Bảng đệm (Kafka engine) + MATERIALIZED VIEW cho mỗi metric.\n"
        "--   topic Kafka -> <m>_kafka -> <m>_mv -> <m>\n"
        "-- Bảng Kafka đọc một lần là mất: đừng SELECT thẳng vào nó khi MV đang\n"
        "-- chạy, sẽ cướp dữ liệu của MV."
    )
    parts = [_HEADER.format(purpose=purpose)]
    for ds in ch_datasets(datasets):
        parts.append(f"\n-- {ds.urn}\n{render_kafka_table(ds)}\n\n{render_mv(ds)}\n")
    return "".join(parts)


def targets(datasets: list[Dataset]) -> dict[str, str]:
    if not ch_datasets(datasets):
        return {}
    return {
        "clickhouse/init/01_schema.sql": render_schema_file(datasets),
        "clickhouse/init/02_kafka_consumers.sql": render_consumers_file(datasets),
    }
