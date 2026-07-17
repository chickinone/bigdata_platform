"""Sinh publication SQL của Postgres từ dataset contract.

Đây là NỬA CÒN LẠI của việc diệt sprawl #2/#3. Cùng với debezium.py, cả hai giờ
đọc CHUNG một nguồn (danh sách dataset CDC), nên `table.include.list` (connector)
và `FOR TABLE ...` (publication) KHÔNG THỂ lệch nhau nữa.

Trước đây: hai file khai tay độc lập. Lệch một bảng = Debezium subscribe bảng
không có trong publication → bảng đó IM LẶNG không có CDC, không lỗi.

Artifact này là TEXT (SQL), không phải JSON. Nó được control plane sở hữu hoàn
toàn, nên `check` so nguyên văn (xem cli._compare).
"""
from __future__ import annotations

from ..registry import Dataset
from .debezium import PUBLICATION_NAME, cdc_datasets

# Header đánh dấu file sinh tự động. Giống dlq_topics.json — để không ai sửa tay
# artifact thay vì sửa contract (đó là drift).
_HEADER = """\
-- =====================================================================
-- FILE SINH TỰ ĐỘNG — đừng sửa tay.
--   Nguồn:    metadata/datasets/*.yaml (mọi dataset có source.type=cdc_debezium)
--   Sinh lại: python -m dataplatform.cli write
--
-- Publication = declarative API của Postgres để publish bảng cho logical
-- replication; Debezium subscribe vào đây. Danh sách bảng dưới đây được GỘP từ
-- registry, nên luôn khớp table.include.list của connector (diệt sprawl #2/#3).
--
-- KHÔNG dùng "FOR ALL TABLES" vì:
--   1. Rủi ro bảo mật — bảng nhạy cảm mới tạo tự động bị publish.
--   2. Khó audit "Debezium đang đọc bảng nào".
--   3. Explicit is better than implicit.
-- =====================================================================
"""


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
