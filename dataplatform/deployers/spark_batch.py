"""Deployer batch Spark medallion — sinh job plan từ metadata rồi spark-submit.

    python -m dataplatform.deployers.spark_batch plan     # sinh + xem, không submit
    python -m dataplatform.deployers.spark_batch apply    # sinh + spark-submit theo thứ tự layer

Thay việc submit tay `enrich_transactions.py` / `build_gold_layer.py`. Batch spec
(inputs + SQL + output) sinh ra job plan JSON; runner mỏng `medallion_runner.py`
thực thi (ADR-0024). Chạy theo thứ tự layer (silver trước gold) vì gold đọc silver.

Cùng triết lý deployer khác: control plane sinh, data plane thực thi; thêm bảng lake
= thêm YAML, không đụng Python.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys

import yaml

from ..registry import REPO_ROOT, ContractError

BATCH_DIR = REPO_ROOT / "metadata" / "pipelines" / "batch"
SPARK_CONTAINER = "bigdata-spark-master"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_MASTER = "spark://spark-master:7077"
PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
ICEBERG_PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0," + PACKAGES
CONTAINER_RUNNER = "/opt/spark-jobs/medallion_runner.py"
PLAN_DIR_HOST = "spark/jobs/generated"
PLAN_DIR_CONTAINER = "/opt/spark-jobs/generated"


def _stage(spec: dict) -> int:
    """Thứ tự chạy theo phụ thuộc INPUT, không theo layer: job đọc Bronze là nguồn
    (chạy trước), job đọc Silver là dẫn xuất (chạy sau — gold + iceberg đều đọc Silver).
    """
    paths = " ".join(i["path"] for i in spec["inputs"])
    if "data-lake-bronze" in paths:
        return 0
    if "data-lake-silver" in paths:
        return 1
    return 2


def _packages(spec: dict) -> str:
    return ICEBERG_PACKAGES if spec["output"].get("format") == "iceberg" else PACKAGES


def load_batch_specs() -> list[dict]:
    specs = []
    for path in sorted(BATCH_DIR.rglob("*.yaml")):
        spec = yaml.safe_load(path.read_text(encoding="utf-8"))
        if spec.get("engine") == "spark_sql":
            specs.append(spec)
    return sorted(specs, key=lambda s: (_stage(s), s["name"]))


def container_plan_path(spec: dict) -> str:
    """Đường dẫn job plan TRONG container (nơi spark-submit đọc)."""
    return f"{PLAN_DIR_CONTAINER}/{spec['name']}.json"


def submit_argv(spec: dict) -> list[str]:
    """Lệnh `docker exec ... spark-submit` chạy một batch job — một nguồn sự thật cho
    'chạy job thế nào'. Deployer dùng để submit; generator Airflow dùng để dựng
    bash_command của task (cùng một lệnh -> DAG chạy y hệt tay/deployer)."""
    return [
        "docker", "exec", "-e", f"JOB_PLAN={container_plan_path(spec)}", SPARK_CONTAINER,
        SPARK_SUBMIT, "--master", SPARK_MASTER,
        # ivy về /tmp: thư mục mặc định không ghi được khi container fresh.
        "--conf", "spark.jars.ivy=/tmp/.ivy2",
        "--packages", _packages(spec), CONTAINER_RUNNER,
    ]


def _write_plan(spec: dict) -> str:
    """Ghi job plan JSON, trả về đường dẫn TRONG container."""
    plan = {
        "name": spec["name"],
        "inputs": spec["inputs"],
        "sql": spec["sql"],
        "output": spec["output"],
    }
    rel = f"{PLAN_DIR_HOST}/{spec['name']}.json"
    path = REPO_ROOT / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8", newline="\n")
    return container_plan_path(spec)


def cmd_plan() -> int:
    specs = load_batch_specs()
    print(f"Đã sinh {len(specs)} batch job plan (thứ tự chạy):\n")
    for spec in specs:
        _write_plan(spec)
        ins = ", ".join(i["view"] for i in spec["inputs"])
        out = spec["output"]
        target = out.get("table") or out.get("path")
        fmt = out.get("format", "parquet")
        print(f"  [stage {_stage(spec)}] {spec['name']}")
        print(f"           inputs: {ins}")
        print(f"           output: {target} ({fmt}, {len(out.get('columns', []))} cột)")
    print("\nChạy `apply` để spark-submit theo thứ tự.")
    return 0


def _submit(spec: dict) -> bool:
    _write_plan(spec)
    print(f"  spark-submit {spec['name']} (layer {spec.get('layer')}) ...")
    proc = subprocess.run(submit_argv(spec), capture_output=True, text=True)
    out = (proc.stdout + proc.stderr).splitlines()
    wrote = [ln for ln in out if ln.startswith("WROTE")]
    if proc.returncode == 0 and wrote:
        print(f"    {wrote[0]}")
        return True
    print(f"    LỖI (exit {proc.returncode}):")
    print("      " + "\n      ".join(out[-8:]))
    return False


def cmd_apply() -> int:
    specs = load_batch_specs()
    print(f"Chạy {len(specs)} batch job theo thứ tự layer:\n")
    failed = 0
    for spec in specs:
        if not _submit(spec):
            failed += 1
            print("    -> dừng chuỗi (job sau có thể phụ thuộc job này).")
            break
    print()
    if failed:
        print(f"KẾT QUẢ: có job LỖI — xem trên.")
        return 1
    print(f"KẾT QUẢ: {len(specs)} batch job chạy xong.")
    return 0


def _force_utf8() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    _force_utf8()
    parser = argparse.ArgumentParser(prog="dataplatform.deployers.spark_batch")
    parser.add_argument("command", nargs="?", default="plan", choices=["plan", "apply"])
    args = parser.parse_args(argv)
    try:
        return {"plan": cmd_plan, "apply": cmd_apply}[args.command]()
    except (ContractError, KeyError) as exc:
        print(f"LỖI SPEC\n{exc}", file=sys.stderr)
        return 2
    except FileNotFoundError as exc:
        print(f"KHÔNG chạy được docker: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
