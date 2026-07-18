"""Deployer runner metric Flink — sinh job plan từ metadata rồi submit.

    python -m dataplatform.deployers.flink_metrics plan     # sinh + xem, không submit
    python -m dataplatform.deployers.flink_metrics apply    # sinh + submit vào Flink

Thay việc submit tay `flink run -py lane1_dashboard.py`. Job plan (source DDL + sink
DDL + INSERT) sinh trên host từ pipeline spec + contract (ADR-0023), ghi ra file
runtime rồi runner mỏng trong container thực thi.

Cùng triết lý với connector deployer (ADR-0021): control plane sinh, data plane
thực thi; thêm/sửa metric = sửa YAML, không đụng Python.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys

from ..generators import flink_sql
from ..registry import REPO_ROOT, ContractError

# URL nội bộ mạng compose (job chạy TRONG container Flink).
BOOTSTRAP = "kafka:9092"
SCHEMA_REGISTRY = "http://schema-registry:8081"
GROUP_ID = "flink-metrics-runner"
STARTUP = "earliest-offset"

FLINK_CONTAINER = "bigdata-flink-jobmanager"
# Đường dẫn TRONG container (flink/jobs mount vào /opt/flink/jobs).
CONTAINER_RUNNER = "/opt/flink/jobs/metric_runner.py"
PLAN_REL = "flink/jobs/generated/metrics-job.json"


def _build_and_write() -> dict:
    job = flink_sql.build_job(
        bootstrap=BOOTSTRAP, schema_registry=SCHEMA_REGISTRY,
        group_id=GROUP_ID, startup=STARTUP,
    )
    path = REPO_ROOT / PLAN_REL
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(job, ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8", newline="\n")
    return job


def cmd_plan() -> int:
    job = _build_and_write()
    print(f"Job plan sinh xong ({PLAN_REL}):")
    print(f"  source: 1 bảng ({len(job['source_ddl'].splitlines())} dòng DDL)")
    print(f"  sinks:  {len(job['sink_ddls'])}")
    print(f"  inserts:{len(job['inserts'])}  | group.id = {job['group_id']}")
    print("\nChạy `apply` để submit runner vào Flink.")
    return 0


def cmd_apply() -> int:
    _build_and_write()
    print(f"Submit runner vào {FLINK_CONTAINER} ...")
    # subprocess (không qua shell) nên đường dẫn container KHÔNG bị MSYS mangle.
    proc = subprocess.run(
        ["docker", "exec", FLINK_CONTAINER, "flink", "run", "-d", "-py", CONTAINER_RUNNER],
        capture_output=True, text=True,
    )
    out = proc.stdout + proc.stderr
    job_line = [ln for ln in out.splitlines() if "JobID" in ln]
    if proc.returncode == 0 and job_line:
        print(f"  {job_line[0].strip()}")
        print("KẾT QUẢ: đã submit. Kiểm RUNNING bằng `flink list`.")
        return 0
    print("LỖI submit:")
    print("  " + "\n  ".join(out.strip().splitlines()[-8:]))
    return 1


def _force_utf8() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    _force_utf8()
    parser = argparse.ArgumentParser(prog="dataplatform.deployers.flink_metrics")
    parser.add_argument("command", nargs="?", default="plan", choices=["plan", "apply"])
    args = parser.parse_args(argv)
    try:
        return {"plan": cmd_plan, "apply": cmd_apply}[args.command]()
    except ContractError as exc:
        print(f"LỖI CONTRACT\n{exc}", file=sys.stderr)
        return 2
    except FileNotFoundError as exc:
        print(f"KHÔNG chạy được docker: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
