"""Exporter governance: OpenMetadata API -> ClickHouse `governance.*` (cho Superset).

    python -m dataplatform.exporters.om_governance

Superset không chart trực tiếp từ REST API, nên kéo dữ liệu catalog về ClickHouse
(đang là tầng serving OLAP của platform) rồi cho Superset query bằng SQL:

    OM API ──pull──▶ governance.catalog_tables    (snapshot catalog: domain/tier/PII/owner)
                     governance.test_case_results (time-series kết quả quality gate)

Hai bảng, hai tính chất:
  - catalog_tables: snapshot — mỗi lần chạy TRUNCATE + INSERT lại (trạng thái hiện tại).
  - test_case_results: append — ReplacingMergeTree khoá (case_fqn, ts) nên chạy lại
    không nhân đôi điểm dữ liệu cũ.

Nguồn kết quả test là OM (lớp TestCaseResult, ADR-0038) — verifier `quality --push-om`
đẩy vào OM, exporter này kéo ra. Không đọc tắt từ verifier để giữ OM là nơi tập trung
kết quả (kể cả khi sau này có nguồn test khác đẩy vào).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time

from ..deployers.openmetadata import _login, _req

CH_CONTAINER = os.getenv("CLICKHOUSE_CONTAINER", "bigdata-clickhouse")

_DDL = [
    "CREATE DATABASE IF NOT EXISTS governance",
    """CREATE TABLE IF NOT EXISTS governance.catalog_tables (
        fqn            String,
        schema_name    String,
        table_name     String,
        domain         String,
        tier           String,
        sensitivity    String,
        owner          String,
        has_description UInt8,
        n_columns      UInt16,
        n_pii          UInt16,
        n_tests        UInt16,
        snapshot_ts    DateTime DEFAULT now()
    ) ENGINE = MergeTree ORDER BY fqn""",
    """CREATE TABLE IF NOT EXISTS governance.test_case_results (
        ts             DateTime64(3),
        case_fqn       String,
        case_name      String,
        definition     String,
        schema_name    String,
        table_name     String,
        column_name    String,
        domain         String,
        status         String,
        message        String
    ) ENGINE = ReplacingMergeTree ORDER BY (case_fqn, ts)""",
]


def _ch(sql: str, input_text: str | None = None) -> str:
    proc = subprocess.run(
        ["docker", "exec", "-i", CH_CONTAINER, "clickhouse-client", "-q", sql],
        capture_output=True, text=True, encoding="utf-8", input=input_text,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "clickhouse-client lỗi")
    return proc.stdout.strip()


def _insert(table: str, rows: list[dict]) -> None:
    if rows:
        payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)
        _ch(f"INSERT INTO {table} FORMAT JSONEachRow", payload)


def _paged(path: str, token: str) -> list[dict]:
    """GET có phân trang kiểu OM (after cursor)."""
    out, after = [], None
    while True:
        url = path + (f"&after={after}" if after else "")
        code, payload = _req("GET", url, token)
        if code != 200:
            raise RuntimeError(f"GET {url} ({code})")
        out += payload.get("data", [])
        after = payload.get("paging", {}).get("after")
        if not after:
            return out


def _tag(tags: list[dict], prefix: str) -> str:
    for t in tags or []:
        if t["tagFQN"].startswith(prefix + "."):
            return t["tagFQN"].split(".", 1)[1]
    return ""


def export() -> int:
    token = _login()
    for ddl in _DDL:
        _ch(ddl)

    # --- catalog snapshot ---
    tables = _paged("/api/v1/tables?limit=100&fields=tags,domains,owners,columns", token)
    # đếm test case theo bảng (fqn case = <table_fqn>.<cột>.<tên>)
    cases = _paged("/api/v1/dataQuality/testCases?limit=100&fields=testDefinition", token)
    n_tests: dict[str, int] = {}
    for c in cases:
        tbl = c["entityFQN"].rsplit(".", 1)[0] if c.get("entityFQN") else \
              c["fullyQualifiedName"].rsplit(".", 2)[0]
        n_tests[tbl] = n_tests.get(tbl, 0) + 1

    rows = []
    for t in tables:
        fqn = t["fullyQualifiedName"]
        cols = t.get("columns", [])
        rows.append({
            "fqn": fqn,
            "schema_name": fqn.split(".")[2] if fqn.count(".") >= 3 else "",
            "table_name": t["name"],
            "domain": (t.get("domains") or [{}])[0].get("name", ""),
            "tier": _tag(t.get("tags"), "Tier"),
            "sensitivity": _tag(t.get("tags"), "Sensitivity"),
            "owner": (t.get("owners") or [{}])[0].get("name", ""),
            "has_description": 1 if (t.get("description") or "").strip() else 0,
            "n_columns": len(cols),
            "n_pii": sum(1 for c in cols if _tag(c.get("tags"), "PII")),
            "n_tests": n_tests.get(fqn, 0),
        })
    _ch("TRUNCATE TABLE governance.catalog_tables")
    _insert("governance.catalog_tables", rows)
    print(f"catalog_tables: {len(rows)} bảng (snapshot).")

    # --- test case results (time-series) ---
    now_ms = int(time.time() * 1000)
    result_rows = []
    for c in cases:
        fqn = c["fullyQualifiedName"]
        parts = fqn.split(".")           # bdp.bank.<schema>.<table>.<cột>.<tên case>
        schema_name, table_name, column_name = parts[2], parts[3], parts[4] if len(parts) > 5 else ""
        code, payload = _req(
            "GET", f"/api/v1/dataQuality/testCases/testCaseResults/{fqn}?startTs=0&endTs={now_ms}", token)
        if code != 200:
            continue
        tbl_fqn = ".".join(parts[:4])
        domain = next((r["domain"] for r in rows if r["fqn"] == tbl_fqn), "")
        for res in payload.get("data", []):
            ms = res["timestamp"]
            ts_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ms / 1000)) + f".{ms % 1000:03d}"
            result_rows.append({
                "ts": ts_str,
                "case_fqn": fqn,
                "case_name": c["name"],
                "definition": (c.get("testDefinition") or {}).get("name", ""),
                "schema_name": schema_name,
                "table_name": table_name,
                "column_name": column_name,
                "domain": domain,
                "status": res["testCaseStatus"],
                "message": res.get("result", ""),
            })
    _insert("governance.test_case_results", result_rows)
    n = _ch("SELECT count() FROM governance.test_case_results FINAL")
    print(f"test_case_results: +{len(result_rows)} điểm kéo về, {n} điểm sau dedup.")
    return 0


def _force_utf8() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


def main() -> int:
    _force_utf8()
    try:
        return export()
    except (RuntimeError, OSError) as exc:
        print(f"LỖI: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
