"""Data quality gate — chạy luật chất lượng trên dữ liệu thật, fail thì chặn promote (Pha 7).

    python -m dataplatform.verifiers.quality            # chạy mọi luật, exit 1 nếu có vi phạm
    python -m dataplatform.verifiers.quality --push-om  # + đẩy TestCaseResult lên OpenMetadata

Hai nguồn luật:
  - tự SUY từ contract: `not_null` cho mọi cột `nullable:false`, `unique` cho `primary_key`.
    Không khai lại — contract đã nói, quality thực thi.
  - tường MINH trong `metadata/quality/*.yaml`: `range`, `accepted_values` — thứ contract
    chỉ mô tả bằng comment, nay thành luật chạy được.

Route theo layer: oltp -> Postgres (`schema.table`), metric -> ClickHouse (`db.table`).
Mỗi check là một câu đếm VI phạm; > 0 là fail. Nguồn không chạy -> SKIP (không thể kiểm).
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


# ---------- dựng câu check (trả về (nhãn, sql) — sql đếm VI phạm) ----------
def _q(v: str) -> str:
    return "'" + v.replace("'", "''") + "'"


def case_specs(ds: Dataset, rules: list[dict]) -> list[dict]:
    """Danh sách test case chuẩn của một dataset — nguồn dùng chung.

    Đây là nơi duy nhất định nghĩa "dataset này có những check nào". Hai consumer:
      - verifier này (dựng SQL đếm vi phạm, chạy trên nguồn thật);
      - deployer OpenMetadata (dựng TestCase + đẩy TestCaseResult, ADR-0038).
    Mỗi spec: {kind, column, params, label, om_name} — om_name là tên TestCase
    trong OM, phải ổn định vì FQN kết quả treo vào nó.
    """
    tname = ds.urn.split(".")[-1]
    specs: list[dict] = []

    def _add(kind: str, col: str, params: dict) -> None:
        specs.append({
            "kind": kind, "column": col, "params": params,
            "label": f"{kind}({col})",
            "om_name": f"{tname}_{col}_{kind}",
        })

    # tự suy: not_null cho mọi cột nullable:false + unique cho PK
    for col in ds.columns():
        if not col.get("nullable", True):
            _add("not_null", col["name"], {})
    if ds.primary_key:
        _add("unique", ds.primary_key, {})
    # tường minh từ metadata/quality
    for r in rules:
        if r["type"] in ("not_null", "unique"):
            _add(r["type"], r["column"], {})
        elif r["type"] == "range":
            _add("range", r["column"], {"min": r["min"], "max": r["max"]})
        elif r["type"] == "accepted_values":
            _add("accepted_values", r["column"], {"values": r["values"]})
    return specs


def _sql_for(table: str, spec: dict) -> str:
    """Câu SQL đếm vi phạm cho một spec (chung cú pháp Postgres/ClickHouse)."""
    col, kind, p = spec["column"], spec["kind"], spec["params"]
    if kind == "not_null":
        return f"SELECT count(*) FROM {table} WHERE {col} IS NULL"
    if kind == "unique":
        return f"SELECT count(*) FROM (SELECT {col} FROM {table} GROUP BY {col} HAVING count(*) > 1) AS _dup"
    if kind == "accepted_values":
        vals = ", ".join(_q(v) for v in p["values"])
        return f"SELECT count(*) FROM {table} WHERE {col} NOT IN ({vals}) AND {col} IS NOT NULL"
    if kind == "range":
        return f"SELECT count(*) FROM {table} WHERE {col} < {p['min']} OR {col} > {p['max']}"
    raise RuntimeError(f"kind chưa hỗ trợ: {kind}")


def _checks_for(table: str, ds: Dataset, rules: list[dict]) -> list[tuple[dict, str]]:
    return [(spec, _sql_for(table, spec)) for spec in case_specs(ds, rules)]


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


def _push_results_om(outcomes: list[tuple[Dataset, dict, str, str]]) -> None:
    """Đẩy TestCaseResult (time-series) lên OpenMetadata — lớp thứ 4 của mô hình DQ.

    outcomes: (dataset, spec, status Success/Failed/Aborted, thông điệp).
    FQN test case = <service>.<db>.<layer>.<table>.<cột>.<om_name> — cùng quy ước
    tên với deployer openmetadata (nó tạo case, verifier này gắn kết quả vào).
    Import lười để tránh vòng import (deployer cũng import module này).
    """
    import time as _time
    from ..deployers.openmetadata import DATABASE, SERVICE, _login, _req

    token = _login()
    now = int(_time.time() * 1000)
    ok = err = 0
    for ds, spec, status, message in outcomes:
        tname = ds.urn.split(".")[-1]
        fqn = f"{SERVICE}.{DATABASE}.{ds.raw['layer']}.{tname}.{spec['column']}.{spec['om_name']}"
        code, payload = _req("POST", f"/api/v1/dataQuality/testCases/testCaseResults/{fqn}",
                             token, {"timestamp": now, "testCaseStatus": status, "result": message})
        if code in (200, 201):
            ok += 1
        else:
            err += 1
            print(f"  [CHÚ Ý] push {fqn} ({code}): {json.dumps(payload)[:120]}")
    print(f"Đẩy OM: {ok} kết quả" + (f", {err} lỗi" if err else "") + ".")


def cmd_verify(push_om: bool = False) -> int:
    datasets = load_datasets()
    rules_by_urn = _load_rules()
    fails = skips = passed = 0
    outcomes: list[tuple[Dataset, dict, str, str]] = []

    for ds in sorted(datasets, key=lambda d: d.urn):
        engine, table = _target(ds)
        if engine is None:
            continue
        runner = _pg_scalar if engine == "postgres" else _ch_scalar
        for spec, sql in _checks_for(table, ds, rules_by_urn.get(ds.urn, [])):
            label = spec["label"]
            try:
                violations = runner(sql)
            except RuntimeError as exc:
                print(f"  [SKIP] {ds.urn} :: {label} ({engine} không chạy được: {str(exc)[:60]})")
                skips += 1
                outcomes.append((ds, spec, "Aborted", f"nguồn {engine} không chạy"))
                continue
            if violations > 0:
                print(f"  [FAIL] {ds.urn} :: {label} — {violations} vi phạm")
                fails += 1
                outcomes.append((ds, spec, "Failed", f"{violations} vi phạm"))
            else:
                print(f"  [ OK ] {ds.urn} :: {label}")
                passed += 1
                outcomes.append((ds, spec, "Success", "0 vi phạm"))

    print(f"\nKẾT QUẢ: {passed} đạt, {fails} vi phạm, {skips} bỏ qua (nguồn không chạy).")
    if push_om:
        try:
            _push_results_om(outcomes)
        except Exception as exc:  # noqa: BLE001 — push là phụ, gate là chính
            print(f"  [CHÚ Ý] không đẩy được kết quả lên OM: {str(exc)[:150]}")
    if fails:
        print("Có vi phạm chất lượng -> chặn promote.")
        return 1
    return 0


def _force_utf8() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    _force_utf8()
    args = argv if argv is not None else sys.argv[1:]
    push_om = "--push-om" in args
    try:
        return cmd_verify(push_om=push_om)
    except (RuntimeError, FileNotFoundError) as exc:
        print(f"LỖI: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
