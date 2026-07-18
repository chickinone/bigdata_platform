"""Đối chiếu contract OLTP với schema Postgres THẬT (information_schema).

    python -m dataplatform.verifiers.postgres_schema

Postgres là NGUỒN SỰ THẬT ĐỘC LẬP: contract được reverse-engineer TỪ nó, không phải
ngược lại. Nên đối chiếu contract ↔ Postgres bắt được lỗi mà đối chiếu contract ↔
artifact (vốn cùng sinh từ contract) không thể thấy.

Kiểm 4 thứ cho mỗi dataset CDC:
  1. Tên cột       — contract thiếu cột DB có (pipeline bỏ sót), hoặc contract có cột
                     DB không có (artifact sinh ra tham chiếu cột ma → vỡ).
  2. Kiểu          — kiểu logic của contract có tương thích kiểu Postgres thật không.
  3. Nullable      — contract khai nullable có khớp NOT NULL của DB không.
  4. Primary key   — contract.primary_key có đúng PK thật của bảng không.

KHÔNG cần credential: chạy psql BÊN TRONG container bằng chính env POSTGRES_USER/DB
của nó (`docker exec ... sh -c 'psql -U "$POSTGRES_USER" ...'`). Verifier không bao
giờ cầm mật khẩu.
"""
from __future__ import annotations

import os
import subprocess
import sys

from ..generators.debezium import cdc_datasets
from ..registry import ContractError, Dataset, load_datasets

POSTGRES_CONTAINER = os.getenv("POSTGRES_CONTAINER", "bigdata-source-postgres")

# Kiểu LOGIC -> tập kiểu Postgres CHẤP NHẬN ĐƯỢC. Một logic map tới nhiều kiểu pg vì
# nhiều kiểu pg cùng biểu diễn một khái niệm logic (vd varchar/char/text đều là chuỗi).
# Đây là ánh xạ NGƯỢC với generator: generator đi logic->engine, verifier kiểm
# engine-thật thuộc tập logic cho phép.
_PG_OK = {
    "long": {"bigint"},
    "int": {"integer", "smallint"},
    "string": {"character varying", "character", "text"},
    "boolean": {"boolean"},
    "timestamp": {"timestamp with time zone", "timestamp without time zone"},
    "date": {"date"},
    "double": {"double precision", "real"},
}


class PgColumn:
    __slots__ = ("name", "data_type", "nullable", "precision", "scale")

    def __init__(self, name, data_type, is_nullable, precision, scale):
        self.name = name
        self.data_type = data_type
        self.nullable = is_nullable == "YES"
        self.precision = precision or ""
        self.scale = scale or ""


def _psql(sql: str) -> list[list[str]]:
    """Chạy SQL trong container, trả các dòng đã tách theo '|'. Dùng env container
    nên không cần user/pass ở ngoài. Ném RuntimeError nếu psql lỗi (để không âm thầm
    coi 'không có kết quả' là 'khớp').
    """
    cmd = [
        "docker", "exec", "-i", POSTGRES_CONTAINER, "sh", "-c",
        'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA -F"|" -f -',
    ]
    proc = subprocess.run(cmd, input=sql, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"psql lỗi (container {POSTGRES_CONTAINER}):\n{proc.stderr.strip()}")
    rows = []
    for line in proc.stdout.splitlines():
        line = line.rstrip("\r")
        if line:
            rows.append(line.split("|"))
    return rows


def _db_columns(schema: str, table: str) -> dict[str, PgColumn]:
    sql = (
        "SELECT column_name, data_type, is_nullable, "
        "coalesce(numeric_precision::text,''), coalesce(numeric_scale::text,'') "
        "FROM information_schema.columns "
        f"WHERE table_schema='{schema}' AND table_name='{table}' "
        "ORDER BY ordinal_position;"
    )
    cols = {}
    for r in _psql(sql):
        # -tA -F'|' có thể nuốt cột rỗng cuối -> đệm cho đủ 5.
        r = (r + ["", "", "", "", ""])[:5]
        cols[r[0]] = PgColumn(*r)
    return cols


def _db_primary_key(schema: str, table: str) -> list[str]:
    sql = (
        "SELECT a.attname FROM pg_index i "
        "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
        f"WHERE i.indrelid='{schema}.{table}'::regclass AND i.indisprimary "
        "ORDER BY a.attname;"
    )
    return [r[0] for r in _psql(sql)]


def _type_ok(logical: str, col: PgColumn) -> bool:
    """Kiểu logic của contract có tương thích kiểu Postgres thật không."""
    if logical.startswith("decimal("):
        p, s = logical[len("decimal("):-1].split(",")
        return col.data_type == "numeric" and col.precision == p.strip() and col.scale == s.strip()
    return col.data_type in _PG_OK.get(logical, set())


def verify_dataset(ds: Dataset) -> tuple[list[str], list[str]]:
    """Trả (errors, warnings). error = sẽ làm vỡ artifact sinh; warning = lệch đáng
    chú ý nhưng không vỡ ngay.
    """
    schema = ds.raw["source"]["schema_name"]
    table = ds.raw["source"]["table"]
    db_cols = _db_columns(schema, table)
    if not db_cols:
        return [f"bảng {schema}.{table} KHÔNG tồn tại trong Postgres"], []

    errors: list[str] = []
    warnings: list[str] = []

    contract_cols = {c["name"]: c for c in ds.columns()}

    # 1. Cột contract có mà DB không có -> artifact sinh ra tham chiếu cột ma.
    for name in contract_cols:
        if name not in db_cols:
            errors.append(f"cột '{name}' có trong contract nhưng KHÔNG có trong DB")

    # 2. Cột DB có mà contract thiếu -> pipeline bỏ sót cột (thường là warning).
    for name in db_cols:
        if name not in contract_cols:
            warnings.append(f"cột '{name}' có trong DB nhưng contract THIẾU (pipeline bỏ sót)")

    # 3. Cột chung: kiểu + nullable.
    for name, cc in contract_cols.items():
        dc = db_cols.get(name)
        if dc is None:
            continue
        if not _type_ok(cc["type"], dc):
            prec = f"({dc.precision},{dc.scale})" if dc.precision else ""
            errors.append(f"cột '{name}' kiểu lệch: contract={cc['type']} vs DB={dc.data_type}{prec}")
        contract_nullable = cc.get("nullable", True)
        if contract_nullable != dc.nullable:
            warnings.append(
                f"cột '{name}' nullable lệch: contract={contract_nullable} vs DB={'YES' if dc.nullable else 'NO'}"
            )

    # 4. Primary key.
    db_pk = _db_primary_key(schema, table)
    contract_pk = ds.primary_key
    if len(db_pk) > 1:
        errors.append(f"DB có PK GHÉP {db_pk} nhưng contract chỉ khai một primary_key='{contract_pk}'")
    elif db_pk and contract_pk != db_pk[0]:
        errors.append(f"primary_key lệch: contract='{contract_pk}' vs DB='{db_pk[0]}'")
    elif not db_pk:
        warnings.append(f"bảng {schema}.{table} KHÔNG có PK trong DB nhưng contract khai '{contract_pk}'")

    return errors, warnings


def cmd_verify() -> int:
    datasets = cdc_datasets(load_datasets())
    print(f"Đối chiếu {len(datasets)} contract CDC với schema Postgres thật "
          f"(container: {POSTGRES_CONTAINER}):\n")

    total_err = total_warn = 0
    for ds in datasets:
        errors, warnings = verify_dataset(ds)
        total_err += len(errors)
        total_warn += len(warnings)
        if not errors and not warnings:
            print(f"  [KHỚP] {ds.urn}")
            continue
        mark = "LỆCH" if errors else "CHÚ Ý"
        print(f"  [{mark}] {ds.urn}")
        for e in errors:
            print(f"          ✗ {e}")
        for w in warnings:
            print(f"          ~ {w}")

    print()
    print(f"KẾT QUẢ: {total_err} lệch (error), {total_warn} chú ý (warning).")
    if total_err:
        print("Contract KHÔNG khớp nguồn sự thật Postgres — sửa contract cho đúng DB.")
        return 1
    if total_warn:
        print("Không có lệch phá vỡ; có cảnh báo nên xem qua.")
        return 0
    print("Mọi contract CDC khớp schema Postgres thật.")
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
