from __future__ import annotations

import os
import subprocess
import sys

from ..generators import clickhouse_ddl
from ..registry import ContractError, Dataset, load_datasets

CLICKHOUSE_CONTAINER = os.getenv("CLICKHOUSE_CONTAINER", "bigdata-clickhouse")


def _ch_query(sql: str) -> list[list[str]]:
    cmd = ["docker", "exec", "-i", CLICKHOUSE_CONTAINER, "clickhouse-client", "-q", sql]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"clickhouse-client lỗi (container {CLICKHOUSE_CONTAINER}):\n{proc.stderr.strip()}")
    return [line.split("\t") for line in proc.stdout.splitlines() if line]


def _actual_columns(db: str, table: str) -> dict[str, str]:
    sql = (
        f"SELECT name, type FROM system.columns "
        f"WHERE database='{db}' AND table='{table}' ORDER BY position FORMAT TSV"
    )
    return {r[0]: r[1] for r in _ch_query(sql)}


def _expected_columns(ds: Dataset) -> dict[str, str]:
    """Kiểu ClickHouse mà contract MONG đợi ở bảng đích — tính bằng chính hàm của
    generator, nên định nghĩa "đúng" ở đây với ở generator là một.
    """
    spec = clickhouse_ddl._spec(ds)
    overrides = spec.get("column_types", {})
    expected = {
        c["name"]: clickhouse_ddl._ch_type(c, low_cardinality_ok=True, overrides=overrides)
        for c in ds.columns()
    }
    # version_column chỉ có ở bảng đích, render thành DateTime (xem render_target_table).
    version = spec.get("version_column")
    if version:
        expected[version] = "DateTime"
    return expected


def verify_dataset(ds: Dataset) -> list[str]:
    spec = clickhouse_ddl._spec(ds)
    db, table = spec["database"], spec["table"]
    actual = _actual_columns(db, table)
    if not actual:
        return [f"bảng {db}.{table} KHÔNG tồn tại trong ClickHouse (chưa apply DDL?)"]

    expected = _expected_columns(ds)
    errors: list[str] = []

    for name, want in expected.items():
        if name not in actual:
            errors.append(f"cột '{name}' contract mong đợi nhưng KHÔNG có trong bảng")
        elif actual[name] != want:
            errors.append(f"cột '{name}' kiểu lệch: contract kỳ vọng {want} vs ClickHouse {actual[name]}")
    for name in actual:
        if name not in expected:
            errors.append(f"cột '{name}' có trong ClickHouse nhưng contract KHÔNG khai (drift?)")

    return errors


# Bang do runner/he thong tao, khong suy tu contract dataset — khong tinh la thua.
_KHONG_TINH_THUA = {"schema_migrations", "dlq_events", "notification_events"}


def _bang_thua(datasets) -> list:
    """Bang trong `metrics` ma contract KHONG con khai.

    CHI BAO CAO, khong bao gio tu xoa. Day la ranh gioi co chu dinh: connector va file
    sinh thi xoa duoc vi tao lai tu metadata la co ngay, con DROP TABLE la mat du lieu
    khong hoi phuc duoc. Tu dong hoa + khong hoi phuc duoc la ket hop toi te nhat.

    Moi metric sinh ra 3 doi tuong (<ten>, <ten>_kafka, <ten>_mv) nen ta gom ho theo
    ten goc truoc khi so, tranh bao thua gia.
    """
    goc = set()
    for ds in datasets:
        ten = ds.raw["sinks"]["clickhouse"]["table"]
        goc.update({ten, f"{ten}_kafka", f"{ten}_mv"})

    rows = _ch_query("SELECT name FROM system.tables WHERE database = 'metrics' ORDER BY name")
    live = {r[0] for r in rows if r and r[0]}

    thua = []
    for ten in sorted(live - goc):
        goc_ten = ten[:-6] if ten.endswith("_kafka") else (ten[:-3] if ten.endswith("_mv") else ten)
        if goc_ten in _KHONG_TINH_THUA:
            continue
        thua.append(ten)
    return thua


def cmd_verify() -> int:
    datasets = clickhouse_ddl.ch_datasets(load_datasets())
    print(f"Đối chiếu {len(datasets)} contract metric với bảng ClickHouse thật "
          f"(container: {CLICKHOUSE_CONTAINER}):\n")

    total = 0
    for ds in datasets:
        errors = verify_dataset(ds)
        total += len(errors)
        if not errors:
            print(f"  [KHỚP] {ds.urn}")
            continue
        print(f"  [LỆCH] {ds.urn}")
        for e in errors:
            print(f"          ✗ {e}")

    thua = _bang_thua(datasets)

    print()
    if thua:
        print(f"  [chú ý] {len(thua)} bảng có trong ClickHouse nhưng contract KHÔNG còn khai:")
        for ten in thua:
            print(f"          metrics.{ten}")
        print("          KHÔNG tự xoá — DROP TABLE là mất dữ liệu không hồi phục được.")
        print("          Nếu chắc chắn không cần: docker exec bigdata-clickhouse "
              "clickhouse-client -q 'DROP TABLE metrics.<tên>'")
        print()

    print(f"KẾT QUẢ: {total} lệch, {len(thua)} bảng thừa (chỉ báo).")
    if total:
        print("Bảng ClickHouse đang chạy KHÔNG khớp contract — có drift thủ công, hoặc cần `write` + apply.")
        return 1
    print("Mọi bảng đích ClickHouse khớp contract (không có drift).")
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
        print(f"KHÔNG đối chiếu được với ClickHouse: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
