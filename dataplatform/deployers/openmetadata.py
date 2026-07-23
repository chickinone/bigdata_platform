"""Deployer OpenMetadata — nạp catalog + governance từ metadata vào UI.

    python -m dataplatform.deployers.openmetadata apply

Đọc `lineage/graph.json` (sinh từ metadata, ADR-0026) và registry, PUSH vào
OpenMetadata qua REST. Giữ đúng triết lý "Git là nguồn sự thật, catalog là nơi
tra cứu" — nạp từ metadata, không gõ tay trên UI.

Hai tầng, hai mức nghiêm ngặt:
  - Lõi (service/database/schema/table/lineage): lỗi là dừng — thiếu là catalog vô dụng.
  - Enrichment (domain/tier/owner, classification, test case, metric, dashboard, KPI):
    lỗi in [CHÚ Ý] rồi đi tiếp — API các phần này đổi theo version OM, không để
    một endpoint lệch làm hỏng cả lần nạp.

Idempotent: các field ghép (tags/domains/owners) được replace nguyên khối theo
contract, không append — chạy lại bao nhiêu lần cũng ra cùng trạng thái.

Chỉ chạy khi OpenMetadata bật (compose riêng). Xem openmetadata/README.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

import yaml

from ..registry import METADATA_DIR, REPO_ROOT, load_connections, load_datasets

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


def _patch(path: str, ops: list[dict], token: str, label: str) -> dict:
    """JSON-Patch (RFC 6902) — content-type riêng, khác PUT."""
    data = json.dumps(ops).encode()
    headers = {"Authorization": f"Bearer {token}",
               "Content-Type": "application/json-patch+json"}
    req = urllib.request.Request(f"{OM_URL}{path}", data=data, method="PATCH", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"PATCH {label} lỗi ({e.code}): {e.read().decode()[:300]}")


def _soft(label: str, fn, warnings: list[str]):
    """Chạy một bước enrichment; lỗi thì ghi cảnh báo thay vì dừng cả lần nạp."""
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001 — enrichment không được phép giết core
        warnings.append(f"{label}: {exc}")
        print(f"  [CHÚ Ý] {label}: {str(exc)[:200]}")
        return None


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

    # 3. Tables — dataset. Cột lấy kiểu + mô tả THẬT từ contract (registry),
    #    không còn STRING đồng loạt như bản đầu.
    ds_by_urn = {d.urn: d for d in load_datasets()}
    node_fqn: dict[str, str] = {}
    for n in graph["dataset_nodes"]:
        schema = _schema_of(n)
        tname = _table_name(n["id"])
        contract = ds_by_urn.get(n["id"])
        contract_cols = {c["name"]: c for c in contract.columns()} if contract else {}
        cols = []
        for c in n["columns"]:
            cc = contract_cols.get(c, {})
            col = {"name": c, "dataType": _om_type(cc.get("type", "string"))}
            if cc.get("description"):
                col["description"] = cc["description"]
            if c in n["pii_columns"]:
                col["tags"] = [{"tagFQN": "PII.Sensitive"}]
            cols.append(col)
        desc = n.get("description") or ""
        owner_line = f"Owner: {n['owner']} | layer: {n['layer']} | tags: {', '.join(n['tags'])}"
        _put("/api/v1/tables", {
            "name": tname, "databaseSchema": f"{SERVICE}.{DATABASE}.{schema}",
            "description": f"{desc}\n\n{owner_line}".strip(),
            "columns": cols or [{"name": "_", "dataType": "STRING"}],
        }, token, f"table {tname}")
        node_fqn[n["id"]] = _fqn(schema, tname)

    # 4. Tables — lake node (cột suy từ column_lineage, xem generators/lineage.py).
    for n in graph["lake_nodes"]:
        tname = _table_name(n["id"])
        cols = [{"name": c, "dataType": "STRING"} for c in n.get("columns", [])]
        _put("/api/v1/tables", {
            "name": tname, "databaseSchema": f"{SERVICE}.{DATABASE}.{n['layer']}",
            "description": f"Lake table ({n['layer']}) — sinh từ Spark medallion.",
            "columns": cols or [{"name": "_", "dataType": "STRING"}],
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

    # 5a. Lineage cột: gom column_lineage theo cạnh (node nguồn -> node đích) để đính vào
    #     lineageDetails của cạnh tương ứng. output = "node.col", inputs = ["node.col", ...].
    col_edges: dict[tuple[str, str], list[tuple[list[str], str]]] = {}
    for rec in graph.get("column_lineage", []):
        dst_node, dst_col = rec["output"].rsplit(".", 1)
        by_src: dict[str, list[str]] = {}
        for inp in rec["inputs"]:
            src_node, src_col = inp.rsplit(".", 1)
            by_src.setdefault(src_node, []).append(src_col)
        for src_node, src_cols in by_src.items():
            col_edges.setdefault((src_node, dst_node), []).append((src_cols, dst_col))

    # 5b. Lineage — nay mọi endpoint đều có table (gồm cả đích sink ngoài).
    fqn_to_id: dict[str, str] = {}
    added = col_pairs = 0
    for e in graph["edges"]:
        src, dst = node_fqn.get(e["from"]), node_fqn.get(e["to"])
        if not src or not dst:
            continue
        for fqn in (src, dst):
            if fqn not in fqn_to_id:
                code, t = _req("GET", f"/api/v1/tables/name/{fqn}", token)
                fqn_to_id[fqn] = t["id"]
        edge = {
            "fromEntity": {"id": fqn_to_id[src], "type": "table"},
            "toEntity": {"id": fqn_to_id[dst], "type": "table"},
        }
        cols = col_edges.get((e["from"], e["to"]))
        if cols:
            edge["lineageDetails"] = {"columnsLineage": [
                {"fromColumns": [f"{src}.{c}" for c in from_cols], "toColumn": f"{dst}.{to_col}"}
                for from_cols, to_col in cols
            ]}
            col_pairs += len(cols)
        _put("/api/v1/lineage", {"edge": edge}, token, f"lineage {e['from']}->{e['to']}")
        added += 1

    print(f"Lõi nạp xong: {added} cạnh lineage ({col_pairs} liên kết cột). Enrichment...\n")
    warnings: list[str] = []

    # 6. Teams từ owner của contract — để gán ownership thật (không chỉ ghi vào description).
    team_ids: dict[str, str] = {}

    def _teams():
        owners = sorted({n["owner"] for n in graph["dataset_nodes"]})
        for o in owners:
            r = _put("/api/v1/teams", {"name": o, "teamType": "Group",
                                       "description": f"Từ trường owner của contract ({o})."},
                     token, f"team {o}")
            team_ids[o] = r["id"]
        print(f"  [OK ] teams: {len(team_ids)}")
    _soft("teams", _teams, warnings)

    # 7. Domains từ trường `domain` của contract.
    domain_ids: dict[str, str] = {}

    def _domains():
        doms = sorted({n["domain"] for n in graph["dataset_nodes"] if n.get("domain")})
        for d in doms:
            r = _put("/api/v1/domains", {
                "name": d, "domainType": "Source-aligned",
                "description": f"Domain khai trong dataset contract (`domain: {d}`).",
            }, token, f"domain {d}")
            domain_ids[d] = r["id"]
        print(f"  [OK ] domains: {len(domain_ids)}")
    _soft("domains", _domains, warnings)

    # 8. Classification "Sensitivity" — tag mức nhạy cảm theo layer.
    def _classification():
        _put("/api/v1/classifications", {
            "name": "Sensitivity",
            "description": "Mức nhạy cảm dữ liệu (suy theo layer + PII của contract).",
        }, token, "classification Sensitivity")
        for t, d in [("Internal", "Dữ liệu nội bộ (metric, lake)."),
                     ("Confidential", "Chứa thông tin khách hàng (OLTP có PII)."),
                     ("Restricted", "Cảnh báo/điều tra — hạn chế truy cập.")]:
            _put("/api/v1/tags", {"name": t, "classification": "Sensitivity", "description": d},
                 token, f"tag Sensitivity.{t}")
        print("  [OK ] classification: Sensitivity (3 tag)")
    _soft("classification", _classification, warnings)

    _SENSITIVITY = {"oltp": "Confidential", "metric": "Internal", "alert": "Restricted"}

    # 9. Gán domain + tier + sensitivity + owner cho từng table (PATCH replace nguyên
    #    khối -> idempotent, chạy lại không nhân đôi tag).
    def _governance_patch():
        n_ok = 0
        for n in graph["dataset_nodes"]:
            fqn = node_fqn[n["id"]]
            code, t = _req("GET", f"/api/v1/tables/name/{fqn}", token)
            if code != 200:
                raise RuntimeError(f"GET {fqn} ({code})")
            ops = []
            tags = []
            if n.get("tier"):
                tags.append({"tagFQN": f"Tier.{n['tier']}"})
            sens = _SENSITIVITY.get(n["layer"])
            if sens:
                tags.append({"tagFQN": f"Sensitivity.{sens}"})
            if tags:
                ops.append({"op": "add", "path": "/tags", "value": tags})
            if n.get("domain") and n["domain"] in domain_ids:
                ops.append({"op": "add", "path": "/domains",
                            "value": [{"id": domain_ids[n["domain"]], "type": "domain"}]})
            if n["owner"] in team_ids:
                ops.append({"op": "add", "path": "/owners",
                            "value": [{"id": team_ids[n["owner"]], "type": "team"}]})
            if ops:
                _patch(f"/api/v1/tables/{t['id']}", ops, token, f"governance {fqn}")
                n_ok += 1
        print(f"  [OK ] domain/tier/sensitivity/owner: {n_ok} table")
    _soft("governance-patch", _governance_patch, warnings)

    # 10. Test case — mô hình DQ 4 lớp của OM, đủ cả 4 (ADR-0038):
    #     TestDefinition : dùng built-in của OM, map từ kind của quality spec.
    #     TestCase       : sinh từ CÙNG nguồn luật với verifiers/quality (case_specs,
    #                      không khai lại) — basic suite per-table OM tự tạo.
    #     TestSuite      : logical suite gom theo domain (bước 10b).
    #     TestCaseResult : verifiers/quality --push-om đẩy sau mỗi lần chạy gate.
    _DEFINITION = {"not_null": "columnValuesToBeNotNull", "unique": "columnValuesToBeUnique",
                   "range": "columnValuesToBeBetween", "accepted_values": "columnValuesToBeInSet"}
    case_ids_by_domain: dict[str, list[str]] = {}

    def _params(spec: dict) -> list[dict]:
        if spec["kind"] == "range":
            return [{"name": "minValue", "value": str(spec["params"]["min"])},
                    {"name": "maxValue", "value": str(spec["params"]["max"])}]
        if spec["kind"] == "accepted_values":
            return [{"name": "allowedValues", "value": json.dumps(spec["params"]["values"])}]
        return []

    def _test_cases():
        from ..verifiers.quality import _load_rules, case_specs
        explicit = _load_rules()
        n_case = 0
        for n in graph["dataset_nodes"]:
            ds = ds_by_urn.get(n["id"])
            if ds is None:
                continue
            fqn = node_fqn[n["id"]]
            for spec in case_specs(ds, explicit.get(n["id"], [])):
                body = {
                    "name": spec["om_name"],
                    "description": (f"`{spec['label']}` của `{n['id']}` — sinh từ contract/quality, "
                                    f"cùng luật với verifiers/quality."),
                    "testDefinition": _DEFINITION[spec["kind"]],
                    "entityLink": f"<#E::table::{fqn}::columns::{spec['column']}>",
                }
                params = _params(spec)
                if params:
                    body["parameterValues"] = params
                code, payload = _req("PUT", "/api/v1/dataQuality/testCases", token, body)
                if code not in (200, 201):
                    raise RuntimeError(f"testCase {body['name']} ({code}): {json.dumps(payload)[:200]}")
                if n.get("domain"):
                    case_ids_by_domain.setdefault(n["domain"], []).append(payload["id"])
                n_case += 1
        print(f"  [OK ] test case: {n_case}")
    _soft("test-cases", _test_cases, warnings)

    # 10b. Logical test suite theo domain — nhìn chất lượng của cả miền nghiệp vụ
    #      (khác basic suite per-table mà OM tự tạo khi thêm test case).
    def _logical_suites():
        n_suite = 0
        for domain, ids in sorted(case_ids_by_domain.items()):
            suite = _put("/api/v1/dataQuality/testSuites", {
                "name": f"{domain}-quality-suite",
                "displayName": f"Quality — {domain}",
                "description": f"Logical suite gom mọi test case của domain `{domain}` (từ contract).",
            }, token, f"testSuite {domain}")
            code, payload = _req("PUT", "/api/v1/dataQuality/testCases/logicalTestCases", token,
                                 {"testSuiteId": suite["id"], "testCaseIds": ids})
            if code not in (200, 201):
                raise RuntimeError(f"logicalTestCases {domain} ({code}): {json.dumps(payload)[:200]}")
            n_suite += 1
        print(f"  [OK ] logical suite: {n_suite} (theo domain)")
    _soft("logical-suites", _logical_suites, warnings)

    # 11. Metric entity (Governance > Metrics) từ aggregations của stream pipeline spec.
    def _metrics():
        n_metric = 0
        for path in sorted((METADATA_DIR / "pipelines" / "stream").glob("*.yaml")):
            spec = yaml.safe_load(path.read_text(encoding="utf-8"))
            for agg in spec.get("aggregations", []):
                expr = agg["expr"]
                up = expr.upper()
                mtype = ("PERCENTAGE" if "rate" in agg["as"] else
                         "COUNT" if up.startswith("COUNT") else
                         "SUM" if up.startswith("SUM") else "OTHER")
                w = spec.get("window", {})
                _put("/api/v1/metrics", {
                    "name": f"{spec['name']}_{agg['as']}",
                    "description": (f"Từ pipeline `{spec['name']}` (window {w.get('type')} "
                                    f"{w.get('size', '')}). Sink: `{spec.get('sink_urn')}`."),
                    "metricExpression": {"code": expr, "language": "SQL"},
                    "metricType": mtype,
                }, token, f"metric {spec['name']}_{agg['as']}")
                n_metric += 1
        print(f"  [OK ] metric entity: {n_metric}")
    _soft("metrics", _metrics, warnings)

    # 12. Grafana dashboard + lineage bảng ClickHouse -> dashboard (từ connection registry).
    def _grafana():
        conns = [c for c in load_connections() if c["type"] == "grafana" and c.get("dashboards")]
        n_dash = n_edge = 0
        for conn in conns:
            url = conn["endpoints"]["url"]
            _put("/api/v1/services/dashboardServices", {
                "name": "grafana", "serviceType": "Grafana",
                "description": conn.get("description", ""),
                "connection": {"config": {"type": "Grafana", "hostPort": url,
                                          "apiKey": "managed-outside-om"}},
            }, token, "dashboardService grafana")
            for d in conn["dashboards"]:
                src = f"{url}/d/{d['uid']}" if d.get("uid") else url
                dash = _put("/api/v1/dashboards", {
                    "name": d["name"], "displayName": d["title"],
                    "service": "grafana", "sourceUrl": src,
                    "description": f"Khai trong `metadata/connections/{conn['name']}` — dashboard đọc ClickHouse serving.",
                }, token, f"dashboard {d['name']}")
                n_dash += 1
                for urn in d.get("tables", []):
                    ds = ds_by_urn.get(urn)
                    if ds is None:
                        continue
                    ch = ds.raw.get("sinks", {}).get("clickhouse")
                    node_id = (f"clickhouse:{ch['database']}.{ch['table']}" if ch else urn)
                    fqn = node_fqn.get(node_id) or node_fqn.get(urn)
                    if not fqn:
                        continue
                    code, t = _req("GET", f"/api/v1/tables/name/{fqn}", token)
                    if code != 200:
                        continue
                    _put("/api/v1/lineage", {"edge": {
                        "fromEntity": {"id": t["id"], "type": "table"},
                        "toEntity": {"id": dash["id"], "type": "dashboard"},
                    }}, token, f"lineage {fqn}->{d['name']}")
                    n_edge += 1
        print(f"  [OK ] grafana: {n_dash} dashboard, {n_edge} cạnh lineage")
    _soft("grafana", _grafana, warnings)

    # 13. KPI cho Data Insights — đo chính catalog: coverage description + ownership.
    def _kpis():
        now = int(time.time() * 1000)
        quarter = 90 * 24 * 3600 * 1000
        for name, chart, target, label in [
            ("description-coverage", "percentage_of_data_asset_with_description_kpi", 80,
             "80% asset có mô tả"),
            ("ownership-coverage", "percentage_of_data_asset_with_owner_kpi", 90,
             "90% asset có owner"),
        ]:
            code, payload = _req("PUT", "/api/v1/kpi", token, {
                "name": name, "displayName": label,
                "description": f"KPI governance: {label}. Nguồn mô tả/owner là contract.",
                "dataInsightChart": chart, "metricType": "PERCENTAGE",
                "targetValue": target, "startDate": now, "endDate": now + quarter,
            })
            if code not in (200, 201):
                raise RuntimeError(f"kpi {name} ({code}): {json.dumps(payload)[:200]}")
        print("  [OK ] KPI insights: 2")
    _soft("kpis", _kpis, warnings)

    status = "đủ" if not warnings else f"thiếu {len(warnings)} phần (xem [CHÚ Ý] trên)"
    print(f"\nKẾT QUẢ: lõi nạp xong ({added} cạnh, {col_pairs} liên kết cột); enrichment {status}. UI: {OM_URL}")
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
