"""Sinh lineage graph + data catalog từ metadata — Pha 6 (discovery/lineage).

Trả lời ba câu hỏi Pha 6, thuần từ metadata (không cần chạy engine):
  - "cột `amount` chảy tới đâu?"        -> lineage cột (Flink) + graph dataset
  - "dataset nào chứa PII?"             -> catalog đánh dấu cột pii
  - "ai sở hữu dataset này?"            -> catalog owner

Ghép mọi cạnh từ các nguồn metadata đã có:
  - dataset.sinks           -> dataset chảy tới ES / S3-bronze / ClickHouse
  - pipeline stream (Flink) -> source_urn -> sink_urn (+ cột lineage từ expr)
  - pipeline batch (Spark)  -> input (bronze topic / silver) -> output (silver/gold/iceberg)

Đầu ra: `lineage/graph.json` (máy đọc, feed DataHub sau) + `lineage/LINEAGE.md` (người đọc).
Cả hai sinh lại từ metadata nên `check` giữ chúng đồng bộ.
"""
from __future__ import annotations

import json
import re

from sqlglot import exp, parse_one
from sqlglot.lineage import lineage as _sql_lineage

from ..registry import Dataset

_AFTER = re.compile(r"`?after`?\.(\w+)")


# ---------- node helpers ----------
def _dataset_nodes(datasets: list[Dataset]) -> list[dict]:
    nodes = []
    for d in datasets:
        cols = d.columns()
        nodes.append({
            "id": d.urn,
            "layer": d.raw["layer"],
            "kind": d.raw["kind"],
            "owner": d.raw["owner"],
            "connection": d.raw["source"].get("connection"),
            "topic": d.topic,
            "columns": [c["name"] for c in cols],
            "pii_columns": [c["name"] for c in cols if c.get("pii")],
            "tags": d.raw.get("tags", []),
        })
    return nodes


def _lake_ref(path_or_table: str) -> tuple[str, str, str] | None:
    """(node_id, layer, label) cho một path/table lake, hoặc None nếu là bronze topic."""
    if not path_or_table.startswith("s3a://"):  # bảng iceberg dạng lakehouse.silver.x
        return (f"iceberg:{path_or_table}", "iceberg", path_or_table)
    parts = [p for p in path_or_table.replace("s3a://", "").split("/") if p]
    bucket, name = parts[0], parts[-1]
    if "silver" in bucket:
        return (f"silver:{name}", "silver", name)
    if "gold" in bucket:
        return (f"gold:{name}", "gold", name)
    return None  # bronze -> map sang dataset qua topic


# ---------- edges ----------
def _sink_edges(datasets: list[Dataset]) -> list[dict]:
    edges = []
    for d in datasets:
        if d.sink_enabled("elasticsearch"):
            edges.append({"from": d.urn, "to": f"es:{d.entity}", "via": f"es-sink-{d.entity}", "kind": "sink"})
        if d.sink_enabled("s3_bronze"):
            edges.append({"from": d.urn, "to": "s3:data-lake-bronze", "via": "s3-sink-cdc", "kind": "sink"})
        ch = d.raw.get("sinks", {}).get("clickhouse")
        if ch and ch.get("enabled"):
            edges.append({"from": d.urn, "to": f"clickhouse:{ch['database']}.{ch['table']}",
                          "via": "ch-kafka-engine", "kind": "sink"})
    return edges


def _stream_edges(pipelines: list[dict]) -> list[dict]:
    edges = []
    for p in pipelines:
        if p.get("engine") in ("flink_sql", "flink_datastream") and p.get("source_urn"):
            edges.append({"from": p["source_urn"], "to": p["sink_urn"],
                          "via": f"flink:{p['name']}", "kind": "stream"})
    return edges


def _batch_edges(batch_specs: list[dict], topic_to_urn: dict[str, str]) -> tuple[list[dict], list[dict]]:
    """Trả (edges, lake_nodes). Input bronze -> dataset urn; silver/gold/iceberg -> lake node."""
    edges, lake_nodes, seen = [], [], set()

    def add_lake(ref):
        nid, layer, label = ref
        if nid not in seen:
            seen.add(nid)
            lake_nodes.append({"id": nid, "layer": layer, "label": label})
        return nid

    for s in batch_specs:
        out = s["output"]
        out_ref = _lake_ref(out.get("table") or out["path"])
        out_id = add_lake(out_ref)
        for inp in s["inputs"]:
            ref = _lake_ref(inp["path"])
            if ref is None:  # bronze topic -> dataset
                topic = [p for p in inp["path"].split("/") if p][-1]
                src = topic_to_urn.get(topic, f"bronze:{topic}")
            else:
                src = add_lake(ref)
            edges.append({"from": src, "to": out_id, "via": f"spark:{s['name']}", "kind": "batch"})
    return edges, lake_nodes


# ---------- column lineage (Flink) ----------
def _flink_column_lineage(pipelines: list[dict]) -> list[dict]:
    out = []
    for p in pipelines:
        if p.get("engine") != "flink_sql":
            continue
        for col in p.get("dimensions", []) + p.get("aggregations", []):
            inputs = sorted({f"{p['source_urn']}.{c}" for c in _AFTER.findall(col["expr"])})
            out.append({
                "engine": "flink", "pipeline": p["name"],
                "output": f"{p['sink_urn']}.{col['as']}",
                "expr": col["expr"],
                "inputs": inputs,
            })
    return out


# ---------- column lineage (Spark) — parse SQL bằng sqlglot ----------
def _batch_out_node(out: dict) -> str:
    """Node id đích của một batch spec (khớp _lake_ref dùng ở _batch_edges)."""
    return _lake_ref(out.get("table") or out["path"])[0]


def _batch_in_node(path: str, topic_to_urn: dict[str, str]) -> str:
    """Node id nguồn: bronze topic -> dataset urn; silver/gold/iceberg -> lake node."""
    ref = _lake_ref(path)
    if ref is None:
        topic = [p for p in path.split("/") if p][-1]
        return topic_to_urn.get(topic, f"bronze:{topic}")
    return ref[0]


def _spark_column_lineage(batch_specs: list[dict], datasets: list[Dataset],
                          topic_to_urn: dict[str, str]) -> list[dict]:
    """Lineage cột cho batch Spark. sqlglot parse SQL (kể cả CTE/join) và lần mỗi cột
    output về cột nguồn. Cấp `schema` (cột từng input) để sqlglot expand `SELECT *` và
    giải cột không định danh bảng. Cột nguồn/đích quy về đúng node id của graph."""
    # node id -> tên cột: dataset (bronze) + đích lake (từ output.columns của spec sinh ra nó).
    node_cols: dict[str, list[str]] = {d.urn: [c["name"] for c in d.columns()] for d in datasets}
    for s in batch_specs:
        node_cols[_batch_out_node(s["output"])] = [c["name"] for c in s["output"].get("columns", [])]

    recs = []
    for s in batch_specs:
        out_id = _batch_out_node(s["output"])
        view_node = {inp["view"]: _batch_in_node(inp["path"], topic_to_urn) for inp in s["inputs"]}
        schema = {v: {c: "STRING" for c in node_cols.get(nid, [])} for v, nid in view_node.items()}
        parsed = parse_one(s["sql"], dialect="spark")
        # alias bảng -> tên thật (chuẩn hoá qualifier như `t.` về đúng view).
        alias_to_table = {t.alias_or_name: t.name for t in parsed.find_all(exp.Table)}

        def to_node(qualifier: str) -> str | None:
            view = qualifier if qualifier in view_node else alias_to_table.get(qualifier)
            return view_node.get(view)

        # SELECT * -> passthrough toàn bộ cột của input đầu tiên.
        if any(isinstance(p, exp.Star) for p in parsed.selects):
            sid = view_node[s["inputs"][0]["view"]]
            for c in node_cols.get(sid, []):
                recs.append({"engine": "spark", "pipeline": s["name"],
                             "output": f"{out_id}.{c}", "expr": "*", "inputs": [f"{sid}.{c}"]})
            continue

        for proj in parsed.selects:
            out_col = proj.alias_or_name
            node = _sql_lineage(out_col, s["sql"], schema=schema, dialect="spark")
            inputs = set()
            for leaf in (n for n in node.walk() if not n.downstream):
                if "." not in leaf.name:      # `*` chưa giải được -> bỏ (hiện là "không nguồn").
                    continue
                qual, col = leaf.name.split(".", 1)
                nid = to_node(qual)
                if nid:
                    inputs.add(f"{nid}.{col}")
            recs.append({"engine": "spark", "pipeline": s["name"],
                         "output": f"{out_id}.{out_col}", "expr": proj.sql(dialect="spark"),
                         "inputs": sorted(inputs)})
    return recs


# ---------- build ----------
def build_graph(datasets: list[Dataset], pipelines: list[dict], batch_specs: list[dict]) -> dict:
    topic_to_urn = {d.topic: d.urn for d in datasets}
    batch_edges, lake_nodes = _batch_edges(batch_specs, topic_to_urn)
    column_lineage = (_flink_column_lineage(pipelines)
                      + _spark_column_lineage(batch_specs, datasets, topic_to_urn))
    # Gán cột cho lake node từ chính column_lineage (cột nào là đích của một transform) —
    # để catalog tạo lake table có cột thật + đẩy được lineage cấp cột. Tự nhất quán: cột
    # tạo ra đúng bằng cột mà lineage tham chiếu.
    lake_cols: dict[str, set[str]] = {}
    for rec in column_lineage:
        node, col = rec["output"].rsplit(".", 1)
        lake_cols.setdefault(node, set()).add(col)
    for n in lake_nodes:
        n["columns"] = sorted(lake_cols.get(n["id"], []))
    return {
        "_comment": "FILE SINH TỰ ĐỘNG - đừng sửa tay. Nguồn: metadata/. Sinh lại: python -m dataplatform.cli write",
        "dataset_nodes": _dataset_nodes(datasets),
        "lake_nodes": sorted(lake_nodes, key=lambda n: n["id"]),
        "edges": sorted(
            _sink_edges(datasets) + _stream_edges(pipelines) + batch_edges,
            key=lambda e: (e["from"], e["to"]),
        ),
        "column_lineage": column_lineage,
    }


# ---------- render markdown ----------
def _mermaid(graph: dict) -> str:
    lines = ["```mermaid", "flowchart LR"]
    for e in graph["edges"]:
        eng = e["via"].split(":", 1)[0]
        lines.append(f'  {_mid(e["from"])} -->|{eng}| {_mid(e["to"])}')
    lines.append("```")
    return "\n".join(lines)


def _mid(node_id: str) -> str:
    """ID mermaid an toàn + nhãn."""
    safe = re.sub(r"[^A-Za-z0-9]", "_", node_id)
    return f'{safe}["{node_id}"]'


def render_report(graph: dict) -> str:
    parts = [
        "# Lineage & Data Catalog\n",
        "> FILE SINH TỰ ĐỘNG từ `metadata/` — đừng sửa tay. Sinh lại: `python -m dataplatform.cli write`.\n",
        "## 1. Sơ đồ dòng chảy dữ liệu\n",
        _mermaid(graph),
        "\n\n## 2. Data catalog — ai sở hữu, PII ở đâu\n",
        "| Dataset | Layer | Owner | Cột PII | Tags |",
        "|---|---|---|---|---|",
    ]
    for n in graph["dataset_nodes"]:
        pii = ", ".join(n["pii_columns"]) or "—"
        tags = ", ".join(n["tags"]) or "—"
        parts.append(f'| `{n["id"]}` | {n["layer"]} | {n["owner"]} | {pii} | {tags} |')

    # PII flow: cột PII chảy tới đâu (theo cạnh sink của dataset chứa PII)
    parts.append("\n## 3. PII chảy tới đâu\n")
    pii_ds = {n["id"]: n["pii_columns"] for n in graph["dataset_nodes"] if n["pii_columns"]}
    if pii_ds:
        parts.append("| Dataset PII | Cột | Chảy tới |")
        parts.append("|---|---|---|")
        for urn, cols in pii_ds.items():
            dests = sorted({e["to"] for e in graph["edges"] if e["from"] == urn})
            parts.append(f'| `{urn}` | {", ".join(cols)} | {", ".join(dests) or "—"} |')
    else:
        parts.append("_Không dataset nào đánh dấu PII._")

    parts.append("\n## 4. Lineage cột — cột đầu ra bắt nguồn từ cột nào\n")
    _column_section(parts, graph, "flink", "4.1 Flink (streaming metric) — từ `expr` trong pipeline")
    _column_section(parts, graph, "spark", "4.2 Spark (batch medallion) — sqlglot parse SQL, lần qua CTE/join")
    return "\n".join(parts) + "\n"


def _column_section(parts: list[str], graph: dict, engine: str, title: str) -> None:
    rows = [c for c in graph["column_lineage"] if c.get("engine") == engine]
    parts.append(f"\n### {title}\n")
    if not rows:
        parts.append("_Không có._")
        return
    parts.append("| Pipeline | Cột đầu ra | Từ cột nguồn | Biểu thức |")
    parts.append("|---|---|---|---|")
    for cl in rows:
        src = ", ".join(f'`{i}`' for i in cl["inputs"]) or "— (không cột nguồn cụ thể)"
        parts.append(f'| {cl["pipeline"]} | `{cl["output"]}` | {src} | `{cl["expr"]}` |')


def targets(datasets: list[Dataset], pipelines: list[dict]) -> dict[str, str]:
    batch_specs = [p for p in pipelines if p.get("engine") == "spark_sql"]
    graph = build_graph(datasets, pipelines, batch_specs)
    return {
        "lineage/graph.json": json.dumps(graph, ensure_ascii=False, indent=2) + "\n",
        "lineage/LINEAGE.md": render_report(graph),
    }
