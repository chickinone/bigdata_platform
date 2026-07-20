"""Deployer migration ClickHouse — versioned, KHÔNG init-once (Pha 7).

    python -m dataplatform.deployers.clickhouse_migrate plan    # xem migration chờ
    python -m dataplatform.deployers.clickhouse_migrate apply   # áp migration chờ

VẤN ĐỀ init-once: `clickhouse/init/*.sql` (schema metric SINH TỪ CONTRACT) chỉ chạy
lúc DB MỚI. DB đang sống mà đổi contract — thêm cột vào metric cũ, thêm bảng infra —
thì `CREATE ... IF NOT EXISTS` KHÔNG đụng bảng đã tồn tại, nên thay đổi không tới nơi.

HAI LỚP tách bạch, mỗi lớp làm đúng việc:

  BASELINE (khai báo) — `clickhouse/init/*.sql`, sinh từ contract, idempotent. Là mầm
    CÀI MỚI: bảng metric mới = tự có. KHÔNG do runner này áp (nó có bảng Kafka-engine
    cần broker sống; cài mới chạy lúc dựng stack).

  MIGRATION (mệnh lệnh, versioned) — `migrations/clickhouse/NNNN_*.sql`. Thay đổi
    INCREMENTAL mà IF NOT EXISTS không làm được: `ALTER TABLE ADD COLUMN`, bảng infra
    mới, backfill. Runner này lo lớp đó: áp MỘT LẦN theo thứ tự, ghi
    `metrics.schema_migrations`, BẤT BIẾN (sửa file đã áp = lỗi). Áp được lên DB SỐNG.

Idempotent: chạy lại chỉ áp phần chưa áp. Kiểm chứng cuối vẫn là verifier
`clickhouse_schema` (live vs contract).
"""
from __future__ import annotations

import hashlib
import os
import subprocess
import sys

from ..registry import REPO_ROOT

CLICKHOUSE_CONTAINER = os.getenv("CLICKHOUSE_CONTAINER", "bigdata-clickhouse")
MIGRATIONS_DIR = REPO_ROOT / "migrations" / "clickhouse"
LEDGER = "metrics.schema_migrations"


def _ch_exec(sql: str) -> str:
    """Chạy SQL (đa câu) trong container ClickHouse. Ném lỗi nếu thất bại."""
    proc = subprocess.run(
        ["docker", "exec", "-i", CLICKHOUSE_CONTAINER, "clickhouse-client", "--multiquery"],
        input=sql, capture_output=True, text=True, encoding="utf-8",
    )
    if proc.returncode != 0:
        raise RuntimeError(f"clickhouse-client lỗi (container {CLICKHOUSE_CONTAINER}):\n{proc.stderr.strip()}")
    return proc.stdout


def _checksum(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _ensure_ledger() -> None:
    """DB metrics + bảng sổ cái migration. ReplacingMergeTree(applied_at): một dòng/version."""
    _ch_exec(
        "CREATE DATABASE IF NOT EXISTS metrics;\n"
        f"CREATE TABLE IF NOT EXISTS {LEDGER} (\n"
        "    version    String,\n"
        "    name       String,\n"
        "    checksum   String,\n"
        "    applied_at DateTime64(3) DEFAULT now64(3)\n"
        ") ENGINE = ReplacingMergeTree(applied_at) ORDER BY version;\n"
    )


def _applied() -> dict[str, str]:
    """{version -> checksum} đã ghi (FINAL để gộp ReplacingMergeTree)."""
    out = _ch_exec(f"SELECT version, checksum FROM {LEDGER} FINAL FORMAT TabSeparated;")
    rows = {}
    for line in out.splitlines():
        if "\t" in line:
            v, c = line.split("\t", 1)
            rows[v] = c
    return rows


def _record(version: str, name: str, checksum: str) -> None:
    _ch_exec(f"INSERT INTO {LEDGER} (version, name, checksum) VALUES "
             f"('{version}', '{name}', '{checksum}');")


def _migration_files() -> list:
    return sorted(MIGRATIONS_DIR.glob("*.sql")) if MIGRATIONS_DIR.exists() else []


def _version_of(path) -> str:
    return path.stem  # "0001_notification_events"


def cmd_plan() -> int:
    try:
        _ensure_ledger()
        applied = _applied()
    except RuntimeError:
        applied = {}  # CH chưa sẵn — vẫn liệt kê được migration
    migs = _migration_files()
    print(f"MIGRATION ClickHouse (áp một lần, bất biến) — {len(migs)} file:\n")
    if not migs:
        print("    (chưa có migration)")
    for p in migs:
        v = _version_of(p)
        state = "đã áp" if v in applied else "CHỜ"
        print(f"    [{state:5}] {p.relative_to(REPO_ROOT)}")
    pending = sum(1 for p in migs if _version_of(p) not in applied)
    print(f"\n{pending} migration CHỜ áp. `apply` để chạy.")
    return 0


def cmd_apply() -> int:
    _ensure_ledger()
    applied = _applied()
    pending = 0
    for path in _migration_files():
        version, sql = _version_of(path), path.read_text(encoding="utf-8")
        checksum = _checksum(sql)
        if version in applied:
            if applied[version] != checksum:
                raise RuntimeError(
                    f"migration '{version}' ĐÃ ÁP nhưng file đổi (checksum lệch) — "
                    f"migration là BẤT BIẾN. Tạo migration MỚI thay vì sửa file cũ."
                )
            continue
        _ch_exec(sql)
        _record(version, path.name, checksum)
        print(f"  [áp   ] {version}")
        pending += 1

    print(f"\nKẾT QUẢ: {pending} migration mới áp"
          f"{' (không có migration chờ — DB đã cập nhật)' if pending == 0 else ''}.")
    return 0


def _force_utf8() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    _force_utf8()
    cmd = (argv or sys.argv[1:] or ["plan"])[0]
    if cmd not in ("plan", "apply"):
        print("dùng: python -m dataplatform.deployers.clickhouse_migrate [plan|apply]", file=sys.stderr)
        return 2
    try:
        return cmd_plan() if cmd == "plan" else cmd_apply()
    except RuntimeError as exc:
        print(f"LỖI: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
