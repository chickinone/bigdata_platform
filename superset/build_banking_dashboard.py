# -*- coding: utf-8 -*-
"""Dashboard "Banking Transaction Analytics" trên Superset — dữ liệu OLTP thật.

    python superset/build_banking_dashboard.py

Bố cục theo mẫu BI ngân hàng: hàng KPI (tổng giao dịch / tổng giá trị / ATV /
tỉ lệ thành công) + amount theo thời gian + giá trị theo loại tài khoản +
khách hàng theo quốc gia (treemap) + phân bố KYC (donut) + trạng thái theo loại.

Nguồn: Postgres OLTP (bankdb) — Superset query thẳng qua network bigdata-net.
Idempotent như build_dashboard.py: có rồi thì dùng lại.
"""
from __future__ import annotations

import json
import sys

from build_dashboard import _env, _find, _login, _req, _sql_metric, ensure_chart

TITLE = "Banking Transaction Analytics"

ENRICHED_SQL = """\
SELECT t.transaction_id, t.amount, t.transaction_type, t.status,
       t.merchant_category, t.created_at,
       a.account_type, a.currency,
       c.country_code, c.kyc_status, c.risk_score
FROM transactions t
JOIN accounts  a ON a.account_id  = t.account_id
JOIN customers c ON c.customer_id = a.customer_id\
"""


def ensure_pg_database(token: str) -> int:
    name = "postgres-oltp"
    existing = _find("/api/v1/database/", "database_name", name, token)
    if existing:
        return existing
    uri = (f"postgresql://{_env('POSTGRES_USER')}:{_env('POSTGRES_PASSWORD')}"
           f"@bigdata-source-postgres:5432/{_env('POSTGRES_DB')}")
    code, r = _req("POST", "/api/v1/database/", token,
                   {"database_name": name, "sqlalchemy_uri": uri})
    if code != 201:
        raise RuntimeError(f"tạo database lỗi ({code}): {r}")
    return r["id"]


def ensure_dataset(db_id: int, table: str, token: str, sql: str | None = None) -> int:
    existing = _find("/api/v1/dataset/", "table_name", table, token)
    if existing:
        return existing
    body = {"database": db_id, "schema": "public", "table_name": table}
    if sql:
        body["sql"] = sql
    code, r = _req("POST", "/api/v1/dataset/", token, body)
    if code != 201:
        raise RuntimeError(f"tạo dataset {table} lỗi ({code}): {r}")
    return r["id"]


def ensure_dashboard(token: str) -> int:
    existing = _find("/api/v1/dashboard/", "dashboard_title", TITLE, token)
    if existing:
        return existing
    code, r = _req("POST", "/api/v1/dashboard/", token,
                   {"dashboard_title": TITLE, "published": True})
    if code != 201:
        raise RuntimeError(f"tạo dashboard lỗi ({code}): {r}")
    return r["id"]


def set_layout(dash_id: int, rows: list[list[tuple[int, str, int]]], token: str) -> None:
    """rows = [[(chart_id, tên, width), ...], ...] — width theo lưới 12 cột."""
    pos: dict = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
    }
    for i, row in enumerate(rows):
        rid = f"ROW-{i}"
        pos["GRID_ID"]["children"].append(rid)
        pos[rid] = {"type": "ROW", "id": rid, "children": [],
                    "parents": ["ROOT_ID", "GRID_ID"],
                    "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        height = 25 if i == 0 else 55        # hàng KPI thấp, hàng chart cao
        for cid, name, width in row:
            key = f"CHART-{cid}"
            pos[rid]["children"].append(key)
            pos[key] = {"type": "CHART", "id": key, "children": [],
                        "parents": ["ROOT_ID", "GRID_ID", rid],
                        "meta": {"chartId": cid, "width": width, "height": height,
                                 "sliceName": name}}
    code, r = _req("PUT", f"/api/v1/dashboard/{dash_id}", token,
                   {"position_json": json.dumps(pos)})
    if code != 200:
        raise RuntimeError(f"set layout lỗi ({code}): {r}")


def main() -> int:
    token = _login()
    db_id = ensure_pg_database(token)
    ds_tx = ensure_dataset(db_id, "transactions", token)
    ds_enr = ensure_dataset(db_id, "transactions_enriched", token, sql=ENRICHED_SQL)
    ds_cus = ensure_dataset(db_id, "customers", token)
    dash_id = ensure_dashboard(token)
    print(f"database={db_id} datasets=({ds_tx},{ds_enr},{ds_cus}) dashboard={dash_id}")

    ids: dict[str, int] = {}

    def add(name, viz, ds, params):
        ids[name] = ensure_chart(name, viz, ds, params, dash_id, token)
        print(f"  chart [{ids[name]}] {name}")

    # --- hàng KPI ---
    add("Tổng giao dịch", "big_number_total", ds_tx,
        {"metric": _sql_metric("count(*)", "giao dịch"), "y_axis_format": ",d"})
    add("Tổng giá trị", "big_number_total", ds_tx,
        {"metric": _sql_metric("sum(amount)", "tổng amount"), "y_axis_format": ".3s"})
    add("Giá trị TB (ATV)", "big_number_total", ds_tx,
        {"metric": _sql_metric("avg(amount)", "ATV"), "y_axis_format": ".2f"})
    add("Tỉ lệ thành công", "big_number_total", ds_tx,
        {"metric": _sql_metric(
            "round(100.0 * count(*) FILTER (WHERE status = 'completed') / count(*), 1)",
            "% completed"),
         "subheader": "status = completed"})

    # --- amount & số giao dịch theo thời gian ---
    add("Amount & giao dịch theo thời gian", "echarts_timeseries_line", ds_tx,
        {"x_axis": "created_at", "time_grain_sqla": "P1D",
         "metrics": [_sql_metric("sum(amount)", "Amount"),
                     _sql_metric("count(*)", "Số giao dịch")],
         "rich_tooltip": True})

    # --- giá trị theo loại tài khoản (vai "Revenue by Product") ---
    add("Giá trị theo loại tài khoản", "echarts_timeseries_bar", ds_enr,
        {"x_axis": "account_type",
         "metrics": [_sql_metric("sum(amount)", "Amount")],
         "y_axis_format": ".3s"})

    # --- khách hàng theo quốc gia (vai "Customers by Location") ---
    add("Khách hàng theo quốc gia", "treemap_v2", ds_cus,
        {"groupby": ["country_code"], "metric": _sql_metric("count(*)", "khách hàng")})

    # --- phân bố KYC (vai "Credit Attribution", donut) ---
    add("Phân bố KYC khách hàng", "pie", ds_cus,
        {"groupby": ["kyc_status"], "metric": _sql_metric("count(*)", "khách hàng"),
         "donut": True, "show_labels_threshold": 0})

    # --- trạng thái theo loại tài khoản (vai "Channel Attribution", stacked) ---
    add("Trạng thái giao dịch theo loại tài khoản", "echarts_timeseries_bar", ds_enr,
        {"x_axis": "account_type", "groupby": ["status"],
         "metrics": [_sql_metric("count(*)", "số giao dịch")],
         "stack": "Stack", "orientation": "horizontal"})

    set_layout(dash_id, [
        [(ids["Tổng giao dịch"], "Tổng giao dịch", 3),
         (ids["Tổng giá trị"], "Tổng giá trị", 3),
         (ids["Giá trị TB (ATV)"], "Giá trị TB (ATV)", 3),
         (ids["Tỉ lệ thành công"], "Tỉ lệ thành công", 3)],
        [(ids["Amount & giao dịch theo thời gian"], "Amount & giao dịch theo thời gian", 8),
         (ids["Phân bố KYC khách hàng"], "Phân bố KYC khách hàng", 4)],
        [(ids["Giá trị theo loại tài khoản"], "Giá trị theo loại tài khoản", 4),
         (ids["Trạng thái giao dịch theo loại tài khoản"], "Trạng thái giao dịch theo loại tài khoản", 4),
         (ids["Khách hàng theo quốc gia"], "Khách hàng theo quốc gia", 4)],
    ], token)
    print(f"\nXong: http://localhost:8088/superset/dashboard/{dash_id}/  (admin/admin)")
    return 0


if __name__ == "__main__":
    for s in (sys.stdout, sys.stderr):
        if hasattr(s, "reconfigure"):
            s.reconfigure(encoding="utf-8")
    raise SystemExit(main())
