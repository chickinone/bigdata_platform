"""Deployer OpenMetadata — nạp lineage graph từ metadata vào catalog UI (Pha 6).

    python -m dataplatform.deployers.openmetadata apply

Đọc `lineage/graph.json` (sinh từ metadata, ADR-0026) và PUSH vào OpenMetadata qua
REST: service -> database -> schema (theo layer) -> table (mỗi dataset/lake node) +
cột + tag PII + lineage. Giữ đúng triết lý "Git là nguồn sự thật, catalog là nơi tra
cứu" — nạp TỪ metadata, không gõ tay trên UI.

Chỉ chạy khi OpenMetadata bật (profile catalog). Xem openmetadata/README.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

from ..registry import REPO_ROOT

OM_URL = os.getenv("OM_URL", "http://localhost:8585")
OM_ADMIN = os.getenv("OM_ADMIN", "admin@open-metadata.org")
OM_PASSWORD_B64 = os.getenv("OM_PASSWORD_B64", "YWRtaW4=")  # "admin"
SERVICE = "bdp"
DATABASE = "bank"

# Prefix node đích ngoài (từ graph.json) -> schema OM.
_SINK_SCHEMA = {"es": "elasticsearch", "s3": "s3", "clickhouse": "clickhouse"}

# Kiểu logic -> OM dataType.
_OM_TYPE = {
    "long": "BIGINT", "int": "INT", "string": "STRING", "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP", "date": "DATE", "double": "DOUBLE",
}


def _om_type(logical: str) -> str:
    if logical.startswith("decimal("):
        return "DECIMAL"
    return _OM_TYPE.get(logical, "STRING")


def _req(method: str, path: str, token: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    # OM dùng content-type khác nhau cho PUT (create-or-update) vs PATCH.
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    req = urllib.request.Request(f"{OM_URL}{path}", data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except json.JSONDecodeError:
            return e.code, {"raw": raw}


def _login() -> str:
    code, payload = _req("POST", "/api/v1/users/login", token="", body={
        "email": OM_ADMIN, "password": OM_PASSWORD_B64,
    })
    if code != 200 or "accessToken" not in payload:
        raise RuntimeError(f"login OM lỗi ({code}): {payload}")
    return payload["accessToken"]


def _put(path: str, body: dict, token: str, label: str) -> dict:
    code, payload = _req("PUT", path, token, body)
    if code not in (200, 201):
        raise RuntimeError(f"PUT {label} lỗi ({code}): {json.dumps(payload)[:300]}")
    return payload


def _schema_of(node: dict) -> str:
    """Schema OM theo layer (oltp/metric/alert/lake...)."""
    return node.get("layer", "lake")


def _table_name(node_id: str) -> str:
    """Tên table = phần cuối urn/id, an toàn."""
    return node_id.split(".")[-1].split(":")[-1]


def _fqn(schema: str, table: str) -> str:
    return f"{SERVICE}.{DATABASE}.{schema}.{table}"


def apply() -> int:
    graph = json.loads((REPO_ROOT / "lineage" / "graph.json").read_text(encoding="utf-8"))
    token = _login()
    print(f"Đăng nhập OM OK. Nạp {len(graph['dataset_nodes'])} dataset + "
          f"{len(graph['lake_nodes'])} lake node + {len(graph['edges'])} cạnh.\n")

    # 1. Service + database.
    _put("/api/v1/services/databaseServices", {
        "name": SERVICE, "serviceType": "CustomDatabase",
        "description": "Big Data Platform — nạp từ metadata (ADR-0026).",
        "connection": {"config": {"type": "CustomDatabase",
                                  "sourcePythonClass": "metadata.ingestion.source.database.customdatabase"}},
    }, token, "service")
    _put("/api/v1/databases", {"name": DATABASE, "service": SERVICE}, token, "database")

    # Đích sink ngoài (es/s3/clickhouse) suy từ endpoint cạnh không phải dataset/lake.
    internal = {n["id"] for n in graph["dataset_nodes"]} | {n["id"] for n in graph["lake_nodes"]}
    externals = sorted({x for e in graph["edges"] for x in (e["from"], e["to"]) if x not in internal})

    # 2. Schemas: layer của dataset/lake + schema của sink ngoài.
    schemas = {_schema_of(n) for n in graph["dataset_nodes"]}
    schemas |= {n["layer"] for n in graph["lake_nodes"]}
    schemas |= {_SINK_SCHEMA.get(x.split(":", 1)[0], "external") for x in externals}
    for s in sorted(schemas):
        _put("/api/v1/databaseSchemas", {"name": s, "database": f"{SERVICE}.{DATABASE}"}, token, f"schema {s}")

    # 3. Tables — dataset (có cột + PII).
    node_fqn: dict[str, str] = {}
    for n in graph["dataset_nodes"]:
        schema = _schema_of(n)
        tname = _table_name(n["id"])
        # cột: cần kiểu; graph.json chỉ có tên cột -> gán STRING (schema thật ở contract).
        cols = []
        for c in n["columns"]:
            col = {"name": c, "dataType": "STRING"}
            if c in n["pii_columns"]:
                col["tags"] = [{"tagFQN": "PII.Sensitive"}]
            cols.append(col)
        owner_desc = f"Owner: {n['owner']} | layer: {n['layer']} | tags: {', '.join(n['tags'])}"
        _put("/api/v1/tables", {
            "name": tname, "databaseSchema": f"{SERVICE}.{DATABASE}.{schema}",
            "description": owner_desc, "columns": cols or [{"name": "_", "dataType": "STRING"}],
        }, token, f"table {tname}")
        node_fqn[n["id"]] = _fqn(schema, tname)

    # 4. Tables — lake node (không cột chi tiết).
    for n in graph["lake_nodes"]:
        tname = _table_name(n["id"])
        _put("/api/v1/tables", {
            "name": tname, "databaseSchema": f"{SERVICE}.{DATABASE}.{n['layer']}",
            "description": f"Lake table ({n['layer']}) — sinh từ Spark medallion.",
            "columns": [{"name": "_", "dataType": "STRING"}],
        }, token, f"lake {tname}")
        node_fqn[n["id"]] = _fqn(n["layer"], tname)

    # 4b. Tables — đích sink ngoài (ES index, ClickHouse table, S3 bucket).
    for nid in externals:
        prefix, rest = nid.split(":", 1)
        schema = _SINK_SCHEMA.get(prefix, "external")
        tname = rest.replace(".", "_").replace("-", "_")
        _put("/api/v1/tables", {
            "name": tname, "databaseSchema": f"{SERVICE}.{DATABASE}.{schema}",
            "description": f"Đích sink ngoài ({prefix}) — {rest}.",
            "columns": [{"name": "_", "dataType": "STRING"}],
        }, token, f"sink {tname}")
        node_fqn[nid] = _fqn(schema, tname)

    # 5. Lineage — nay mọi endpoint đều có table (gồm cả đích sink ngoài).
    fqn_to_id: dict[str, str] = {}
    added = 0
    for e in graph["edges"]:
        src, dst = node_fqn.get(e["from"]), node_fqn.get(e["to"])
        if not src or not dst:
            continue
        for fqn in (src, dst):
            if fqn not in fqn_to_id:
                code, t = _req("GET", f"/api/v1/tables/name/{fqn}", token)
                fqn_to_id[fqn] = t["id"]
        _put("/api/v1/lineage", {"edge": {
            "fromEntity": {"id": fqn_to_id[src], "type": "table"},
            "toEntity": {"id": fqn_to_id[dst], "type": "table"},
        }}, token, f"lineage {e['from']}->{e['to']}")
        added += 1

    print(f"KẾT QUẢ: nạp xong. Lineage cạnh nội bộ: {added}. UI: {OM_URL}")
    return 0


def _force_utf8() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    _force_utf8()
    cmd = (argv or sys.argv[1:] or ["apply"])[0]
    if cmd != "apply":
        print("dùng: python -m dataplatform.deployers.openmetadata apply", file=sys.stderr)
        return 2
    try:
        return apply()
    except (RuntimeError, urllib.error.URLError, FileNotFoundError) as exc:
        print(f"LỖI: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
