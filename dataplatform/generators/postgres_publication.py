from __future__ import annotations

from ..registry import Dataset
from .debezium import PUBLICATION_NAME, cdc_datasets

# Header đánh dấu file sinh tự động. Giống dlq_topics.json — để không ai sửa tay
# artifact thay vì sửa contract (đó là drift).
_HEADER = (
    "-- FILE SINH TỰ ĐỘNG — đừng sửa tay. "
    "Sinh lại: python -m dataplatform.cli write\n"
)


def render(datasets: list[Dataset]) -> str:
    members = cdc_datasets(datasets)
    # Mỗi bảng một dòng, thụt lề — dễ đọc diff khi thêm/bớt bảng.
    table_lines = ",\n".join(
        f'        {d.raw["source"]["schema_name"]}.{d.raw["source"]["table"]}'
        for d in members
    )

    return (
        _HEADER
        + f"\nCREATE PUBLICATION {PUBLICATION_NAME}\n"
        + "    FOR TABLE\n"
        + table_lines
        + "\n    WITH (publish = 'insert, update, delete');\n\n"
        + "-- GRANT tường minh cho replicator (defensive — dù 01_users.sql đã cấp).\n"
        + "GRANT SELECT ON ALL TABLES    IN SCHEMA public TO replicator;\n"
        + "GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO replicator;\n"
    )


def targets(datasets: list[Dataset]) -> dict[str, str]:
    if not cdc_datasets(datasets):
        return {}
    return {"postgres/init/04_publication.sql": render(datasets)}
