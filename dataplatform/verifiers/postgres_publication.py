"""Đối chiếu publication Postgres thật với danh sách dataset CDC trong contract.

    python -m dataplatform.verifiers.postgres_publication

Bịt lỗ hổng cuối của mối nối publication ↔ table.include.list (ADR-0018).

Generator đã bảo đảm hai ARTIFACT không lệch: `04_publication.sql` và
`postgres-connector.json` cùng gọi `debezium.cdc_datasets()`. Nhưng artifact đúng
không có nghĩa Postgres ĐANG CHẠY đúng: `04_publication.sql` là init script, chỉ
chạy khi DB mới tạo. Thêm bảng vào contract trên DB sống thì file trên đĩa đúng,
`cli check` xanh, mà publication thật vẫn thiếu bảng.

Vì sao lệch này nguy hiểm hơn mọi lệch khác trong hệ thống:

  - snapshot ban đầu vẫn chạy (Debezium SELECT thẳng bảng, không qua publication)
    -> bảng CÓ dữ liệu cũ trong Kafka;
  - streaming thì im lặng (pgoutput chỉ giải mã bảng có trong publication)
    -> bảng không bao giờ có bản ghi mới;
  - connector vẫn RUNNING, không exception, không log lỗi.

Tức là hệ thống trông khoẻ mạnh và dữ liệu trông có thật — chỉ là đứng im mãi mãi.
Đây là lớp lỗi KHÔNG thể phát hiện bằng cách nhìn trạng thái; cách duy nhất là hỏi
thẳng Postgres. Đó là việc của file này.

Kiểm 3 thứ:
  1. Publication có tồn tại không, và có phải FOR ALL TABLES không.
  2. Tập bảng: thiếu (contract có, DB không) / thừa (DB có, contract không còn).
  3. Phép được publish: contract sinh `publish = 'insert, update, delete'`.
     Thiếu `delete` -> bản ghi xoá không tới ES/lake, cũng im lặng.

Kèm một kiểm liên quan cùng họ: `source.replica_identity` — cũng khai trong contract,
cũng chưa nơi nào đối chiếu, và cũng hỏng im lặng (khai `full` mà DB là `default` thì
UPDATE/DELETE mất before-image, ADR-0004). Chung một câu hỏi "Postgres có đang được
cấu hình đúng như contract nói không", nên chung một verifier.

Chỉ ĐỌC, không sửa. Muốn tự sửa là việc của deployer/migration — trộn hai vai vào
một công cụ là cách nhanh nhất để có thứ tự ý ALTER mà không ai kiểm soát được.

Không cần credential: tái dùng `_psql` của postgres_schema (chạy trong container
bằng chính env của nó).
"""
from __future__ import annotations

import sys

from ..generators.debezium import PUBLICATION_NAME, cdc_datasets
from ..registry import ContractError, Dataset, load_datasets
from .postgres_schema import POSTGRES_CONTAINER, _psql

# Phép mà generator khai: WITH (publish = 'insert, update, delete').
# Khoá ở đây để nếu generator đổi chính sách thì verifier đỏ, buộc sửa cả hai.
_EXPECTED_OPS = ("insert", "update", "delete")

# pg_class.relreplident -> giá trị `source.replica_identity` trong contract.
_REPLIDENT = {"d": "default", "n": "nothing", "f": "full", "i": "index"}


def _publication_row() -> dict | None:
    """Thông tin publication, hoặc None nếu publication CHƯA tồn tại.

    Phân biệt hai ca rất khác nhau: publication không có (connector sẽ FAILED ngay
    khi khởi động — ồn ào, dễ thấy) vs publication có nhưng thiếu bảng (im lặng).
    """
    rows = _psql(
        "SELECT puballtables, pubinsert, pubupdate, pubdelete "
        f"FROM pg_publication WHERE pubname='{PUBLICATION_NAME}';"
    )
    if not rows:
        return None
    r = (rows[0] + ["", "", "", ""])[:4]
    return {
        "all_tables": r[0] == "t",
        "insert": r[1] == "t",
        "update": r[2] == "t",
        "delete": r[3] == "t",
    }


def _published_tables() -> set[str]:
    """{'schema.table'} mà publication đang thực sự phát."""
    rows = _psql(
        "SELECT schemaname, tablename FROM pg_publication_tables "
        f"WHERE pubname='{PUBLICATION_NAME}' ORDER BY 1, 2;"
    )
    return {f"{r[0]}.{r[1]}" for r in rows if len(r) >= 2}


def _actual_replica_identity(schema: str, table: str) -> str | None:
    """Replica identity thật của bảng, hoặc None nếu bảng không tồn tại."""
    rows = _psql(
        "SELECT c.relreplident FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        f"WHERE n.nspname='{schema}' AND c.relname='{table}' AND c.relkind='r';"
    )
    if not rows:
        return None
    return _REPLIDENT.get(rows[0][0], rows[0][0])


def _contract_tables(datasets: list[Dataset]) -> dict[str, Dataset]:
    """{'schema.table' -> dataset} cho mọi dataset CDC. Cùng cách dựng khoá với
    `debezium.table_include_list`, nên so được trực tiếp."""
    return {
        f'{d.raw["source"]["schema_name"]}.{d.raw["source"]["table"]}': d
        for d in datasets
    }


def verify_publication(datasets: list[Dataset]) -> tuple[list[str], list[str]]:
    """Trả (errors, warnings) cho phần publication."""
    errors: list[str] = []
    warnings: list[str] = []

    pub = _publication_row()
    if pub is None:
        return [
            f"publication '{PUBLICATION_NAME}' KHÔNG tồn tại trong Postgres "
            "— Debezium sẽ FAILED lúc khởi động (publication.autocreate.mode=disabled)."
        ], []

    if pub["all_tables"]:
        # Không phải lỗi vận hành (CDC vẫn chạy), nhưng trái chính sách đã chốt:
        # bảng nhạy cảm mới tạo sẽ tự động bị publish, và mất khả năng audit.
        warnings.append(
            f"publication '{PUBLICATION_NAME}' đang là FOR ALL TABLES — trái chính sách "
            "tường minh của 04_publication.sql (bảng mới tự động bị publish)."
        )

    for op in _EXPECTED_OPS:
        if not pub[op]:
            errors.append(
                f"publication KHÔNG publish '{op}' — sự kiện {op} không bao giờ tới hạ nguồn "
                "(im lặng, không lỗi)."
            )

    want = _contract_tables(datasets)
    have = _published_tables()

    for name in sorted(set(want) - have):
        errors.append(
            f"THIẾU `{name}` trong publication — bảng này có snapshot nhưng KHÔNG bao giờ "
            "có bản ghi mới. Sửa: ALTER PUBLICATION "
            f"{PUBLICATION_NAME} ADD TABLE {name};"
        )

    # Thừa = publication phát bảng không còn contract nào mô tả. Không vỡ gì (Debezium
    # lọc lại bằng table.include.list) nhưng là WAL phí và là dấu hiệu contract bị xoá
    # mà chưa dọn Postgres.
    for name in sorted(have - set(want)):
        warnings.append(
            f"THỪA `{name}` trong publication — không dataset CDC nào mô tả bảng này "
            f"(WAL phí). Sửa: ALTER PUBLICATION {PUBLICATION_NAME} DROP TABLE {name};"
        )

    return errors, warnings


def verify_replica_identity(datasets: list[Dataset]) -> list[str]:
    """Trả danh sách lệch replica identity. Contract không khai -> bỏ qua (không suy
    mặc định hộ, vì 'không khai' khác 'khai default')."""
    msgs: list[str] = []
    for ds in datasets:
        want = ds.raw["source"].get("replica_identity")
        if want is None:
            continue
        schema = ds.raw["source"]["schema_name"]
        table = ds.raw["source"]["table"]
        actual = _actual_replica_identity(schema, table)
        if actual is None:
            msgs.append(f"{schema}.{table}: bảng KHÔNG tồn tại trong Postgres")
        elif actual != want:
            extra = ""
            if want == "full":
                extra = " — UPDATE/DELETE sẽ thiếu before-image (ADR-0004)"
            msgs.append(
                f"{schema}.{table}: replica identity lệch — contract='{want}' "
                f"vs DB='{actual}'{extra}"
            )
    return msgs


def cmd_verify() -> int:
    datasets = cdc_datasets(load_datasets())
    print(
        f"Đối chiếu publication '{PUBLICATION_NAME}' với {len(datasets)} dataset CDC "
        f"(container: {POSTGRES_CONTAINER}):\n"
    )

    errors, warnings = verify_publication(datasets)

    # In từng bảng để thấy trạng thái đầy đủ, không chỉ phần lệch.
    have = _published_tables()
    for name in sorted(_contract_tables(datasets)):
        print(f"  [{'KHỚP ' if name in have else 'THIẾU'}] {name}")

    if errors or warnings:
        print()
    for e in errors:
        print(f"  ✗ {e}")
    for w in warnings:
        print(f"  ~ {w}")

    ri_msgs = verify_replica_identity(datasets)
    print("\nReplica identity:")
    if ri_msgs:
        for m in ri_msgs:
            print(f"  ✗ {m}")
    else:
        print("  [KHỚP ] mọi bảng đúng như contract khai.")

    total_err = len(errors) + len(ri_msgs)
    print()
    print(f"KẾT QUẢ: {total_err} lệch (error), {len(warnings)} chú ý (warning).")
    if total_err:
        print("Postgres KHÔNG khớp contract — chạy các lệnh ALTER gợi ý ở trên rồi kiểm lại.")
        return 1
    if warnings:
        print("Không có lệch phá vỡ; có cảnh báo nên xem qua.")
        return 0
    print("Publication thật khớp tuyệt đối danh sách dataset CDC.")
    return 0


def _force_utf8() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    _force_utf8()
    try:
        return cmd_verify()
    except ContractError as exc:
        print(f"LỖI CONTRACT\n{exc}", file=sys.stderr)
        return 2
    except (RuntimeError, FileNotFoundError) as exc:
        print(f"KHÔNG đối chiếu được với Postgres: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
