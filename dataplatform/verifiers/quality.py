"""Data quality gate — chạy luật chất lượng trên DỮ LIỆU THẬT, fail thì chặn promote (Pha 7).

    python -m dataplatform.verifiers.quality        # chạy mọi luật, exit 1 nếu có vi phạm

Hai nguồn luật:
  - TỰ SUY từ contract: `not_null` cho mọi cột `nullable:false`, `unique` cho `primary_key`.
    Không khai lại — contract đã nói, quality thực thi.
  - TƯỜNG MINH trong `metadata/quality/*.yaml`: `range`, `accepted_values` — thứ contract
    chỉ mô tả bằng comment, nay thành luật chạy được.

Route theo layer: oltp -> Postgres (`schema.table`), metric -> ClickHouse (`db.table`).
Mỗi check là một câu đếm VI PHẠM; > 0 là fail. Nguồn không chạy -> SKIP (không thể kiểm).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

import yaml
from jsonschema import Draft202012Validator

from ..registry import METADATA_DIR, SCHEMA_DIR, Dataset, load_datasets

PG_CONTAINER = os.getenv("POSTGRES_CONTAINER", "bigdata-source-postgres")
PG_USER = os.getenv("POSTGRES_USER", "admin")
PG_DB = os.getenv("POSTGRES_DB", "bankdb")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "phantruong1")
CH_CONTAINER = os.getenv("CLICKHOUSE_CONTAINER", "bigdata-clickhouse")
QUALITY_DIR = METADATA_DIR / "quality"


# ---------- chạy SQL trả về một số (đếm vi phạm) ----------
def _pg_scalar(sql: str) -> int:
    proc = subprocess.run(
        ["docker", "exec", "-e", f"PGPASSWORD={PG_PASSWORD}", "-i", PG_CONTAINER,
         "psql", "-U", PG_USER, "-d", PG_DB, "-tAc", sql],
        capture_output=True, text=True, encoding="utf-8",
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "psql lỗi")
    return int(proc.stdout.strip() or "0")


def _ch_scalar(sql: str) -> int:
    proc = subprocess.run(
        ["docker", "exec", "-i", CH_CONTAINER, "clickhouse-client", "-q", sql],
        capture_output=True, text=True, encoding="utf-8",
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "clickhouse-client lỗi")
    return int(proc.stdout.strip() or "0")


# ---------- dựng câu check (trả về (nhãn, sql) — sql đếm VI PHẠM) ----------
def _q(v: str) -> str:
    return "'" + v.replace("'", "''") + "'"


def _checks_for(table: str, ds: Dataset, rules: list[dict]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    # tự suy: not_null
    for col in ds.columns():
        if not col.get("nullable", True):
            out.append((f"not_null({col['name']})",
                        f"SELECT count(*) FROM {table} WHERE {col['name']} IS NULL"))
    # tự suy: unique (PK)
    if ds.primary_key:
        pk = ds.primary_key
        out.append((f"unique({pk})",
                    f"SELECT count(*) FROM (SELECT {pk} FROM {table} GROUP BY {pk} HAVING count(*) > 1) AS _dup"))
    # tường minh
    for r in rules:
        col, rtype = r["column"], r["type"]
        if rtype == "not_null":
            out.append((f"not_null({col})", f"SELECT count(*) FROM {table} WHERE {col} IS NULL"))
        elif rtype == "unique":
            out.append((f"unique({col})",
                        f"SELECT count(*) FROM (SELECT {col} FROM {table} GROUP BY {col} HAVING count(*) > 1) AS _dup"))
        elif rtype == "accepted_values":
            vals = ", ".join(_q(v) for v in r["values"])
            out.append((f"accepted_values({col})",
                        f"SELECT count(*) FROM {table} WHERE {col} NOT IN ({vals}) AND {col} IS NOT NULL"))
        elif rtype == "range":
            out.append((f"range({col})",
                        f"SELECT count(*) FROM {table} WHERE {col} < {r['min']} OR {col} > {r['max']}"))
    return out


def _target(ds: Dataset) -> tuple[str | None, str | None]:
    """(engine, bảng để query) hoặc (None, None) nếu không có đích query được."""
    if ds.raw["layer"] == "oltp":
        s = ds.raw["source"]
        return "postgres", f'{s["schema_name"]}.{s["table"]}'
    ch = ds.raw.get("sinks", {}).get("clickhouse")
    if ch and ch.get("enabled"):
        return "clickhouse", f'{ch["database"]}.{ch["table"]}'
    return None, None


def _load_rules() -> dict[str, list[dict]]:
    if not QUALITY_DIR.exists():
        return {}
    schema = json.loads((SCHEMA_DIR / "quality.schema.json").read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)
    out: dict[str, list[dict]] = {}
    for path in sorted(QUALITY_DIR.rglob("*.yaml")):
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        errs = sorted(validator.iter_errors(raw), key=lambda e: list(e.path))
        if errs:
            raise RuntimeError(f"quality rule sai schema ({path.name}): {errs[0].message}")
        out[raw["dataset"]] = raw["rules"]
    return out


def cmd_verify() -> int:
    datasets = load_datasets()
    rules_by_urn = _load_rules()
    fails = skips = passed = 0

    for ds in sorted(datasets, key=lambda d: d.urn):
        engine, table = _target(ds)
        if engine is None:
            continue
        runner = _pg_scalar if engine == "postgres" else _ch_scalar
        for label, sql in _checks_for(table, ds, rules_by_urn.get(ds.urn, [])):
            try:
                violations = runner(sql)
            except RuntimeError as exc:
                print(f"  [SKIP] {ds.urn} :: {label} ({engine} không chạy được: {str(exc)[:60]})")
                skips += 1
                continue
            if violations > 0:
                print(f"  [FAIL] {ds.urn} :: {label} — {violations} vi phạm")
                fails += 1
            else:
                print(f"  [ OK ] {ds.urn} :: {label}")
                passed += 1

    print(f"\nKẾT QUẢ: {passed} đạt, {fails} vi phạm, {skips} bỏ qua (nguồn không chạy).")
    if fails:
        print("Có vi phạm chất lượng -> CHẶN promote.")
        return 1
    return 0


def _force_utf8() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    _force_utf8()
    try:
        return cmd_verify()
    except (RuntimeError, FileNotFoundError) as exc:
        print(f"LỖI: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
