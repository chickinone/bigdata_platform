"""Sinh config Debezium source connector từ dataset contract.

Generator fan-in: sinh `table.include.list` bằng cách
gộp mọi dataset CDC. Cùng với generator publication (postgres_publication.py),
nó đóng sprawl #2/#3 — trước đây danh sách bảng CDC bị khai tay ở hai nơi
(publication SQL + connector JSON), lệch một bảng là mất CDC âm thầm.

Sau khi cả hai cùng sinh từ registry, chúng không thể lệch nhau nữa, vì cùng đọc
một nguồn (danh sách dataset CDC).
"""
from __future__ import annotations

from ..registry import Dataset, endpoint

CONNECTOR_NAME = "postgres-source-connector"
CONNECTOR_CLASS = "io.debezium.connector.postgresql.PostgresConnector"

# Prefix topic + tên slot/publication. Đây là các quy ước khoá ở một chỗ.
# Đổi prefix = mọi consumer hạ nguồn phải đổi, nên nó sống ở đây, không rải rác.
TOPIC_PREFIX = "bankdb"
SLOT_NAME = "debezium_slot"
PUBLICATION_NAME = "dbz_publication"


def cdc_datasets(datasets: list[Dataset]) -> list[Dataset]:
    """Dataset nào tham gia CDC. Sắp theo tên bảng để output ổn định."""
    members = [d for d in datasets if d.is_cdc]
    return sorted(members, key=lambda d: d.raw["source"]["table"])


def table_include_list(datasets: list[Dataset]) -> str:
    """Chuỗi `schema.table,schema.table,...` cho Debezium.

    Đây là mẩu sự thật bị chép tay ở 2 nơi. Giờ nó suy từ registry.
    """
    members = cdc_datasets(datasets)
    return ",".join(
        f'{d.raw["source"]["schema_name"]}.{d.raw["source"]["table"]}'
        for d in members
    )


def render(datasets: list[Dataset], conns: dict[str, dict]) -> dict:
    sr = endpoint(conns, "schema_registry", "connect_url")
    config = {
        "connector.class": CONNECTOR_CLASS,
        "tasks.max": "1",

        # Postgres nguồn + role replication đọc từ connection postgres_main.endpoints.
        "database.hostname": endpoint(conns, "postgres_main", "connect_hostname"),
        "database.port": endpoint(conns, "postgres_main", "connect_port"),
        "database.user": endpoint(conns, "postgres_main", "connect_user"),
        "database.password": endpoint(conns, "postgres_main", "connect_password"),
        "database.dbname": endpoint(conns, "postgres_main", "connect_dbname"),

        "plugin.name": "pgoutput",
        "slot.name": SLOT_NAME,
        "publication.name": PUBLICATION_NAME,
        # disabled: Debezium không tự tạo publication. Ta quản publication bằng
        # SQL sinh riêng (postgres_publication.py). Nếu để 'filtered', Debezium
        # tự sửa publication -> có 2 thứ cùng quản 1 publication -> drift.
        "publication.autocreate.mode": "disabled",

        "topic.prefix": TOPIC_PREFIX,
        # <<< mẩu sự thật fan-in: gộp từ mọi dataset CDC
        "table.include.list": table_include_list(datasets),

        "snapshot.mode": "initial",
        # string: mọi NUMERIC -> STRING trên Avro (ADR-0003). Đây là nguồn của
        # encoded_as: string trong contract, và lý do Flink phải CAST.
        "decimal.handling.mode": "string",
        "time.precision.mode": "adaptive",
        "heartbeat.interval.ms": "10000",
        "tombstones.on.delete": "false",

        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": sr,
        "key.converter.apicurio.registry.auto-register": "true",
        "key.converter.apicurio.registry.find-latest": "true",
        "key.converter.apicurio.registry.as-confluent": "true",
        "key.converter.apicurio.registry.id-handler": "io.apicurio.registry.serde.Legacy4ByteIdHandler",

        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": sr,
        "value.converter.apicurio.registry.auto-register": "true",
        "value.converter.apicurio.registry.find-latest": "true",
        "value.converter.apicurio.registry.as-confluent": "true",
        "value.converter.apicurio.registry.id-handler": "io.apicurio.registry.serde.Legacy4ByteIdHandler",

        "schema.name.adjustment.mode": "avro",
    }
    return {"name": CONNECTOR_NAME, "config": config}


def targets(datasets: list[Dataset], conns: dict[str, dict]) -> dict[str, dict]:
    if not cdc_datasets(datasets):
        return {}
    return {"debezium/postgres-connector.json": render(datasets, conns)}
