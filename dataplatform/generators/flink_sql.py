"""Sinh Flink SQL cho pipeline metric streaming — diệt sprawl #6/#8.

ĐÂY LÀ RUNNER TỔNG QUÁT (phần sinh). Thay 4 câu INSERT + source ROW + 4 sink DDL
viết tay trong `flink/jobs/lane1_dashboard.py` bằng: 1 pipeline spec khai báo cho
mỗi metric, sinh ra toàn bộ SQL.

Ba mẩu sinh, mỗi mẩu bịt một chỗ:
  - source DDL  : ROW<...> sinh từ cột contract THẬT SỰ được tham chiếu (diệt sprawl
                  #6 - trước đây ROW lặp tay ở nhiều file). Kiểu theo MÃ HOÁ TRÊN DÂY
                  (amount encoded_as:string -> STRING), không phải kiểu logic.
  - sink DDL    : sinh từ cột contract metric (diệt nửa Flink của sprawl #8 - trước
                  đây sink DDL viết tay, có thể lệch ClickHouse). Cùng nguồn cột với
                  DDL ClickHouse nên KHÔNG THỂ lệch.
  - INSERT SQL  : dựng từ window/filter/dimensions/aggregations/rank của spec.

Kiểm chéo quan trọng: thứ tự cột SELECT = [window_start, window_end] + rank +
dimensions + aggregations, và nó PHẢI KHỚP ĐÚNG cột của contract sink. Nếu spec mô
tả ra tập cột khác sink, generator dừng — spec và sink không thể lệch âm thầm.

Kiến trúc: sinh SQL ở ĐÂY (host, có deps), runner mỏng trong container CHỈ thực thi.
"""
from __future__ import annotations

import re

import yaml

from ..registry import REPO_ROOT, ContractError, Dataset, load_datasets

PIPELINE_DIR = REPO_ROOT / "metadata" / "pipelines"

# Kiểu logic + mã hoá -> kiểu Flink cho SOURCE (đọc Avro trên dây).
# Khác sink: ở đây theo MÃ HOÁ THẬT trên Kafka, không phải kiểu logic.
def _source_type(col: dict) -> str:
    if col.get("encoded_as") == "string":
        return "STRING"                       # decimal.handling.mode=string (ADR-0003)
    t = col["type"]
    if t == "long":
        return "BIGINT"
    if t == "int":
        return "INT"
    if t == "string":
        return "STRING"
    if t == "boolean":
        return "BOOLEAN"
    if t == "double":
        return "DOUBLE"
    if t == "timestamp":
        return "STRING"                       # ZonedTimestamp = string trên dây
    if t == "date":
        return "INT"
    raise ContractError(f"source: kiểu chưa hỗ trợ '{t}' (cột {col['name']})")


# Kiểu logic -> kiểu Flink cho SINK (ghi JSON ra Kafka).
def _sink_type(col: dict) -> str:
    t = col["type"]
    if t.startswith("decimal("):
        p, s = t[len("decimal("):-1].split(",")
        return f"DECIMAL({p.strip()}, {s.strip()})"
    if t in ("long", "int"):
        # int cũng BIGINT: cột nhận COUNT/ROW_NUMBER (Flink trả BIGINT). rank_num logic
        # là int nhưng nhận ROW_NUMBER -> phải BIGINT, khớp lane1 cũ.
        return "BIGINT"
    if t == "timestamp":
        return "TIMESTAMP(3)"
    if t == "string":
        return "STRING"
    if t == "boolean":
        return "BOOLEAN"
    if t == "double":
        return "DOUBLE"
    raise ContractError(f"sink: kiểu chưa hỗ trợ '{t}' (cột {col['name']})")


def load_pipelines() -> list[dict]:
    """Đọc mọi pipeline spec, sắp theo name để output ổn định."""
    specs = []
    for path in sorted(PIPELINE_DIR.rglob("*.yaml")):
        specs.append(yaml.safe_load(path.read_text(encoding="utf-8")))
    return sorted(specs, key=lambda p: p["name"])


def _by_urn(datasets: list[Dataset]) -> dict[str, Dataset]:
    return {d.urn: d for d in datasets}


def _interval(spec_val: str) -> str:
    """'5 MINUTE' -> INTERVAL '5' MINUTE."""
    num, unit = spec_val.split(None, 1)
    return f"INTERVAL '{num}' {unit}"


def _referenced_columns(pipelines: list[dict], source_urn: str) -> list[str]:
    """Cột `after`.X thật sự được các pipeline dùng chung source này tham chiếu.

    Sinh source ROW từ đúng những cột này -> ROW tối thiểu, và tự động loại cột chết
    (ROW viết tay cũ có transaction_id, currency mà không INSERT nào dùng).
    """
    used: set[str] = set()
    pat = re.compile(r"`after`\.(\w+)")
    for p in pipelines:
        if p["source_urn"] != source_urn:
            continue
        blobs = [p.get("filter", "")]
        blobs += [d["expr"] for d in p.get("dimensions", [])]
        blobs += [a["expr"] for a in p.get("aggregations", [])]
        for b in blobs:
            used.update(pat.findall(b))
    return sorted(used)


def render_source_ddl(source: Dataset, referenced: list[str], *, bootstrap: str,
                      schema_registry: str, group_id: str, startup: str) -> str:
    by_name = {c["name"]: c for c in source.columns()}
    row_fields = ", ".join(f"{name} {_source_type(by_name[name])}" for name in referenced)
    table = f"{source.raw['source']['table']}_source"
    return f"""CREATE TABLE {table} (
    op STRING,
    ts_ms BIGINT,
    `after` ROW<{row_fields}>,
    event_time AS TO_TIMESTAMP_LTZ(ts_ms, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '{source.topic}',
    'properties.bootstrap.servers' = '{bootstrap}',
    'properties.group.id' = '{group_id}',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '{schema_registry}',
    'scan.startup.mode' = '{startup}'
)"""


def _sink_table_name(sink: Dataset) -> str:
    return f"{sink.raw['sinks']['clickhouse']['table']}_sink"


def render_sink_ddl(sink: Dataset, *, bootstrap: str) -> str:
    cols = ",\n    ".join(f"{c['name']} {_sink_type(c)}" for c in sink.columns())
    return f"""CREATE TABLE {_sink_table_name(sink)} (
    {cols}
) WITH (
    'connector' = 'kafka',
    'topic' = '{sink.topic}',
    'properties.bootstrap.servers' = '{bootstrap}',
    'format' = 'json',
    'sink.partitioner' = 'fixed'
)"""


def _window_table(pipeline: dict, source_table: str) -> str:
    w = pipeline["window"]
    tc = w["time_col"]
    if w["type"] == "tumble":
        return f"TABLE(TUMBLE(TABLE {source_table}, DESCRIPTOR({tc}), {_interval(w['size'])}))"
    if w["type"] == "cumulate":
        return (f"TABLE(CUMULATE(TABLE {source_table}, DESCRIPTOR({tc}), "
                f"{_interval(w['step'])}, {_interval(w['size'])}))")
    raise ContractError(f"window type chưa hỗ trợ: {w['type']}")


def _assert_columns_match(pipeline: dict, sink: Dataset) -> None:
    """Cột spec sinh ra PHẢI khớp đúng cột contract sink (tên + thứ tự)."""
    produced = ["window_start", "window_end"]
    if pipeline.get("rank"):
        produced.append(pipeline["rank"]["as"])
    produced += [d["as"] for d in pipeline.get("dimensions", [])]
    produced += [a["as"] for a in pipeline["aggregations"]]

    expected = [c["name"] for c in sink.columns()]
    if produced != expected:
        raise ContractError(
            f"pipeline '{pipeline['name']}' sinh ra cột {produced}\n"
            f"  nhưng contract sink {sink.urn} có cột {expected}\n"
            f"  -> spec và sink lệch nhau."
        )


def render_insert(pipeline: dict, source: Dataset, sink: Dataset) -> str:
    _assert_columns_match(pipeline, sink)
    source_table = f"{source.raw['source']['table']}_source"
    sink_table = _sink_table_name(sink)
    dims = pipeline.get("dimensions", [])
    aggs = pipeline["aggregations"]
    filt = pipeline.get("filter")

    dim_select = [f"{d['expr']} AS {d['as']}" for d in dims]
    agg_select = [f"{a['expr']} AS {a['as']}" for a in aggs]
    group_by = ", ".join(["window_start", "window_end"] + [d["expr"] for d in dims])

    base_select = ",\n            ".join(["window_start", "window_end"] + dim_select + agg_select)
    where = f"\n        WHERE {filt}" if filt else ""
    inner = (
        f"SELECT\n            {base_select}\n"
        f"        FROM {_window_table(pipeline, source_table)}{where}\n"
        f"        GROUP BY {group_by}"
    )

    if not pipeline.get("rank"):
        return f"INSERT INTO {sink_table}\n        {inner}"

    # rank: aggregate (inner) -> ROW_NUMBER trong cửa sổ (giữa) -> giữ top keep (ngoài).
    r = pipeline["rank"]
    dim_names = [d["as"] for d in dims]
    agg_names = [a["as"] for a in aggs]
    partition = ", ".join(r["partition_by"])
    ranked_cols = ",\n                ".join(["window_start", "window_end"] + dim_names + agg_names)
    outer_cols = ", ".join(["window_start", "window_end", r["as"]] + dim_names + agg_names)
    return (
        f"INSERT INTO {sink_table}\n"
        f"        SELECT {outer_cols}\n"
        f"        FROM (\n"
        f"            SELECT\n                {ranked_cols},\n"
        f"                ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {r['order_by']}) AS {r['as']}\n"
        f"            FROM (\n                {inner}\n            )\n"
        f"        )\n"
        f"        WHERE {r['as']} <= {r['keep']}"
    )


def build_job(*, bootstrap: str, schema_registry: str, group_id: str, startup: str) -> dict:
    """Job plan đầy đủ: source DDL + sink DDLs + inserts. Runner mỏng chỉ việc thực thi.

    Gộp mọi pipeline chung một source vào MỘT source table + MỘT StatementSet, đúng như
    lane1_dashboard làm thủ công — nay tự động.
    """
    datasets = _by_urn(load_datasets())
    pipelines = [p for p in load_pipelines() if p.get("engine") == "flink_sql"]
    if not pipelines:
        return {}

    source_urns = {p["source_urn"] for p in pipelines}
    if len(source_urns) != 1:
        raise ContractError(f"hiện chỉ hỗ trợ 1 source chung, thấy: {sorted(source_urns)}")
    source = datasets[next(iter(source_urns))]

    referenced = _referenced_columns(pipelines, source.urn)
    source_ddl = render_source_ddl(
        source, referenced, bootstrap=bootstrap, schema_registry=schema_registry,
        group_id=group_id, startup=startup,
    )

    sink_ddls, inserts = [], []
    for p in pipelines:
        sink = datasets[p["sink_urn"]]
        sink_ddls.append(render_sink_ddl(sink, bootstrap=bootstrap))
        inserts.append(render_insert(p, source, sink))

    return {
        "job_name": "metric_runner",
        "group_id": group_id,
        "source_ddl": source_ddl,
        "sink_ddls": sink_ddls,
        "inserts": inserts,
    }
