"""Sinh Airflow DAG orchestration batch medallion từ metadata — Pha 7.

Thứ tự tác vụ KHÔNG khai tay: suy từ **phụ thuộc input/output** của batch spec —
job đọc `data-lake-silver` phụ thuộc job GHI ra nó (silver -> gold/iceberg). Cùng
quan hệ mà `_batch_edges` (lineage) và `_stage` (spark_batch) đã dùng. Thêm/sửa job
= sửa spec, DAG tự đổi; không đụng file DAG.

Mỗi task chạy ĐÚNG lệnh `docker exec ... spark-submit` như deployer `spark_batch`
(qua `submit_argv`) — nên DAG chạy y hệt chạy tay, không có "đường thứ hai" để lệch.

Ingestion CDC (Debezium -> Bronze) là stream CHẠY LIÊN TỤC, không phải task batch —
nên nó là thượng nguồn ngầm của silver (silver đọc Bronze), không nằm trong DAG này.
"""
from __future__ import annotations

from ..deployers.spark_batch import _stage, submit_argv

DAG_ID = "medallion_batch"
_HEADER = '''"""FILE SINH TỰ ĐỘNG - đừng sửa tay. Nguồn: metadata/pipelines/batch/. Sinh lại: python -m dataplatform.cli write.

DAG orchestration medallion batch (Bronze -> Silver -> Gold/Iceberg). Thứ tự task suy
từ phụ thuộc input/output của batch spec. Mỗi task = spark-submit trong container
spark-master (Airflow cần docker CLI + socket; xem airflow/README.md).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Chính sách vận hành mặc định. Đổi ở đây = đổi cho mọi task (một chỗ).
default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=2),
}
'''


def _out_id(spec: dict) -> str:
    out = spec["output"]
    return (out.get("path") or out["table"]).rstrip("/")


def _input_ids(spec: dict) -> list[str]:
    return [i["path"].rstrip("/") for i in spec["inputs"] if "path" in i]


def _ordered_with_deps(specs: list[dict]) -> tuple[list[dict], dict[str, list[str]]]:
    """(specs theo thứ tự ổn định, {task -> upstream tasks}). Upstream = job GHI ra
    input của job này (khớp output_id với input_id)."""
    out_to_name = {_out_id(s): s["name"] for s in specs}
    ordered = sorted(specs, key=lambda s: (_stage(s), s["name"]))
    deps = {
        s["name"]: sorted({out_to_name[i] for i in _input_ids(s) if i in out_to_name})
        for s in ordered
    }
    return ordered, deps


def render(pipelines: list[dict]) -> str:
    specs = [p for p in pipelines if p.get("engine") == "spark_sql"]
    ordered, deps = _ordered_with_deps(specs)

    lines = [_HEADER, "", "with DAG(", f'    dag_id="{DAG_ID}",',
             '    description="Medallion Spark batch — sinh từ metadata (ADR-0031).",',
             '    schedule="@daily",', "    start_date=datetime(2026, 1, 1),",
             "    catchup=False,", "    default_args=default_args,",
             '    tags=["medallion", "spark", "generated"],', ") as dag:"]

    for s in ordered:
        cmd = " ".join(submit_argv(s))
        lines += [
            f'    {s["name"]} = BashOperator(',
            f'        task_id="{s["name"]}",',
            f'        bash_command="{cmd}",',
            "    )",
        ]

    # Phụ thuộc: gom theo thượng nguồn cho gọn (silver >> [gold..., iceberg]).
    downstreams: dict[str, list[str]] = {s["name"]: [] for s in ordered}
    for s in ordered:
        for up in deps[s["name"]]:
            downstreams[up].append(s["name"])

    dep_lines = []
    for s in ordered:
        ds = sorted(downstreams[s["name"]])
        if not ds:
            continue
        target = ds[0] if len(ds) == 1 else "[" + ", ".join(ds) + "]"
        dep_lines.append(f"    {s['name']} >> {target}")
    if dep_lines:
        lines += ["", "    # Phụ thuộc suy từ input/output của batch spec.", *dep_lines]

    return "\n".join(lines) + "\n"


def targets(pipelines: list[dict]) -> dict[str, str]:
    return {f"airflow/dags/{DAG_ID}_dag.py": render(pipelines)}
