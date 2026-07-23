# -*- coding: utf-8 -*-
"""Dựng dashboard Governance trên Superset qua REST API — idempotent.

    python superset/build_dashboard.py

Nguồn dữ liệu: ClickHouse `governance.*` (exporter om_governance kéo từ OpenMetadata).
Tạo: 1 database connection + 2 dataset + 6 chart + 1 dashboard có layout.
Chạy lại: tìm theo tên, có rồi thì dùng lại (không tạo trùng).
"""
from __future__ import annotations

import json
import os
import pathlib
import sys
import urllib.parse
import urllib.request

SUPERSET = os.getenv("SUPERSET_URL", "http://localhost:8088")
REPO = pathlib.Path(__file__).resolve().parent.parent


def _env(key: str) -> str:
    """Đọc .env của repo (không commit secret vào script)."""
    for line in (REPO / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1].strip()
    raise RuntimeError(f"thiếu {key} trong .env")


def _req(method: str, path: str, token: str | None = None, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(f"{SUPERSET}{path}", data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except json.JSONDecodeError:
            return e.code, {"raw": raw[:300]}


def _login() -> str:
    code, r = _req("POST", "/api/v1/security/login",
                   body={"username": "admin", "password": "admin", "provider": "db", "refresh": True})
    if code != 200:
        raise RuntimeError(f"login Superset lỗi ({code}): {r}")
    return r["access_token"]


def _find(path: str, name_col: str, name: str, token: str) -> int | None:
    q = urllib.parse.quote(json.dumps(
        {"filters": [{"col": name_col, "opr": "eq", "value": name}]}))
    code, r = _req("GET", f"{path}?q={q}", token)
    items = r.get("result", [])
    return items[0]["id"] if items else None


def ensure_database(token: str) -> int:
    name = "clickhouse-governance"
    existing = _find("/api/v1/database/", "database_name", name, token)
    if existing:
        return existing
    uri = (f"clickhousedb://{_env('CLICKHOUSE_USER')}:{_env('CLICKHOUSE_PASSWORD')}"
           f"@bigdata-clickhouse:8123/governance")
    code, r = _req("POST", "/api/v1/database/", token,
                   {"database_name": name, "sqlalchemy_uri": uri})
    if code != 201:
        raise RuntimeError(f"tạo database lỗi ({code}): {r}")
    return r["id"]


def ensure_dataset(db_id: int, table: str, token: str) -> int:
    existing = _find("/api/v1/dataset/", "table_name", table, token)
    if existing:
        return existing
    code, r = _req("POST", "/api/v1/dataset/", token,
                   {"database": db_id, "schema": "governance", "table_name": table})
    if code != 201:
        raise RuntimeError(f"tạo dataset {table} lỗi ({code}): {r}")
    return r["id"]


def _sql_metric(expr: str, label: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": expr, "label": label}


def ensure_chart(name: str, viz: str, ds_id: int, params: dict, dash_id: int, token: str) -> int:
    existing = _find("/api/v1/chart/", "slice_name", name, token)
    if existing:
        return existing
    params = {"viz_type": viz, "datasource": f"{ds_id}__table", **params}
    code, r = _req("POST", "/api/v1/chart/", token, {
        "slice_name": name, "viz_type": viz,
        "datasource_id": ds_id, "datasource_type": "table",
        "params": json.dumps(params), "dashboards": [dash_id],
    })
    if code != 201:
        raise RuntimeError(f"tạo chart '{name}' lỗi ({code}): {r}")
    return r["id"]


def ensure_dashboard(token: str) -> int:
    title = "Governance — bigdata-platform"
    existing = _find("/api/v1/dashboard/", "dashboard_title", title, token)
    if existing:
        return existing
    code, r = _req("POST", "/api/v1/dashboard/", token,
                   {"dashboard_title": title, "published": True})
    if code != 201:
        raise RuntimeError(f"tạo dashboard lỗi ({code}): {r}")
    return r["id"]


def set_layout(dash_id: int, chart_ids: list[tuple[int, str]], token: str) -> None:
    """Layout lưới 2 chart/hàng (12 cột -> mỗi chart rộng 6)."""
    pos: dict = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
    }
    row = None
    for i, (cid, name) in enumerate(chart_ids):
        if i % 2 == 0:
            row = f"ROW-{i // 2}"
            pos["GRID_ID"]["children"].append(row)
            pos[row] = {"type": "ROW", "id": row, "children": [],
                        "parents": ["ROOT_ID", "GRID_ID"],
                        "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        key = f"CHART-{cid}"
        pos[row]["children"].append(key)
        pos[key] = {"type": "CHART", "id": key, "children": [],
                    "parents": ["ROOT_ID", "GRID_ID", row],
                    "meta": {"chartId": cid, "width": 6, "height": 50, "sliceName": name}}
    code, r = _req("PUT", f"/api/v1/dashboard/{dash_id}", token,
                   {"position_json": json.dumps(pos)})
    if code != 200:
        raise RuntimeError(f"set layout lỗi ({code}): {r}")


def main() -> int:
    token = _login()
    db_id = ensure_database(token)
    ds_catalog = ensure_dataset(db_id, "catalog_tables", token)
    ds_results = ensure_dataset(db_id, "test_case_results", token)
    dash_id = ensure_dashboard(token)
    print(f"database={db_id} datasets=({ds_catalog},{ds_results}) dashboard={dash_id}")

    charts: list[tuple[int, str]] = []

    def add(name, viz, ds, params):
        cid = ensure_chart(name, viz, ds, params, dash_id, token)
        charts.append((cid, name))
        print(f"  chart [{cid}] {name}")

    add("Số test case", "big_number_total", ds_results,
        {"metric": _sql_metric("count(DISTINCT case_fqn)", "test cases"),
         "subheader": "test case đang theo dõi (từ contract/quality)"})

    add("Tỉ lệ pass (mọi lần chạy)", "big_number_total", ds_results,
        {"metric": _sql_metric("round(countIf(status = 'Success') * 100.0 / count(), 1)", "% pass"),
         "subheader": "% điểm kết quả Success trên toàn bộ time-series"})

    add("Kết quả test theo thời gian", "echarts_timeseries_bar", ds_results,
        {"x_axis": "ts", "time_grain_sqla": "PT1H",
         "metrics": [_sql_metric("count(*)", "số kết quả")],
         "groupby": ["status"], "stack": "Stack"})

    add("Bảng theo domain", "pie", ds_catalog,
        {"groupby": ["domain"], "metric": _sql_metric("count(*)", "số bảng"),
         "adhoc_filters": [{"expressionType": "SQL", "clause": "WHERE",
                            "sqlExpression": "domain <> ''"}]})

    add("PII & test theo bảng", "table", ds_catalog,
        {"query_mode": "aggregate",
         "groupby": ["schema_name", "table_name", "tier", "owner"],
         "metrics": [_sql_metric("sum(n_pii)", "cột PII"),
                     _sql_metric("sum(n_tests)", "test case")],
         "row_limit": 50,
         "adhoc_filters": [{"expressionType": "SQL", "clause": "WHERE",
                            "sqlExpression": "domain <> ''"}]})

    add("Coverage mô tả & owner", "big_number_total", ds_catalog,
        {"metric": _sql_metric("round(avg(has_description) * 100.0, 1)", "% có mô tả"),
         "subheader": "trên toàn bộ bảng trong catalog"})

    set_layout(dash_id, charts, token)
    print(f"\nXong: {SUPERSET}/superset/dashboard/{dash_id}/  (admin/admin)")
    return 0


if __name__ == "__main__":
    for s in (sys.stdout, sys.stderr):
        if hasattr(s, "reconfigure"):
            s.reconfigure(encoding="utf-8")
    raise SystemExit(main())
