"""Deployer job Flink streaming — sinh config từ metadata rồi submit.

    python -m dataplatform.deployers.flink_metrics plan     # sinh + xem, không submit
    python -m dataplatform.deployers.flink_metrics apply    # sinh + submit vào Flink

Deploy CẢ HAI job sinh từ metadata: metric runner (SQL) và fraud runner (DataStream).
Thay việc submit tay `flink run -py lane1_dashboard.py` / `lane3_fraud_detection.py`.
Config sinh trên host từ pipeline spec + contract (ADR-0023), ghi ra file runtime rồi
runner trong container thực thi.

Cùng triết lý với connector deployer (ADR-0021): control plane sinh, data plane thực
thi; thêm/sửa metric hoặc chỉnh ngưỡng fraud = sửa YAML, không đụng Python.

LƯU Ý: `apply` submit MỚI, không huỷ job cũ — nếu job đang chạy thì huỷ trước bằng
`flink cancel` để tránh hai bản cùng ghi. (Chưa có reconcile — Pha 7.)
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys

from ..generators import flink_sql
from ..registry import REPO_ROOT, ContractError, endpoint, connections_by_name

# Endpoint nội bộ mạng compose (job chạy TRONG container Flink) — nay đọc TỪ connection
# registry (kafka.bootstrap + schema_registry.url), không hardcode nữa. Flink nhúng
# literal (không có lớp EnvVarConfigProvider như Kafka Connect) nên dùng dạng `url`.
_CONNS = connections_by_name()
BOOTSTRAP = endpoint(_CONNS, "kafka", "bootstrap")
SCHEMA_REGISTRY = endpoint(_CONNS, "schema_registry", "url")
GROUP_ID = "flink-metrics-runner"
STARTUP = "earliest-offset"

FLINK_CONTAINER = "bigdata-flink-jobmanager"
FRAUD_GROUP_ID = "flink-fraud-runner"
# Đường dẫn TRONG container (flink/jobs mount vào /opt/flink/jobs).
METRIC_RUNNER = "/opt/flink/jobs/metric_runner.py"
FRAUD_RUNNER = "/opt/flink/jobs/fraud_runner.py"
METRIC_PLAN_REL = "flink/jobs/generated/metrics-job.json"
FRAUD_PLAN_REL = "flink/jobs/generated/fraud-job.json"


def _write(rel: str, payload: dict) -> None:
    path = REPO_ROOT / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8", newline="\n")


def _build_and_write() -> tuple[dict, dict]:
    metric = flink_sql.build_job(
        bootstrap=BOOTSTRAP, schema_registry=SCHEMA_REGISTRY,
        group_id=GROUP_ID, startup=STARTUP,
    )
    fraud = flink_sql.build_fraud_config(
        bootstrap=BOOTSTRAP, schema_registry=SCHEMA_REGISTRY, group_id=FRAUD_GROUP_ID,
    )
    _write(METRIC_PLAN_REL, metric)
    _write(FRAUD_PLAN_REL, fraud)
    return metric, fraud


def cmd_plan() -> int:
    metric, fraud = _build_and_write()
    print(f"Đã sinh 2 config:")
    print(f"  {METRIC_PLAN_REL}: {len(metric['sink_ddls'])} sink, {len(metric['inserts'])} insert, "
          f"group {metric['group_id']}")
    print(f"  {FRAUD_PLAN_REL}: fraud '{fraud['job_name']}' -> {fraud['sink_topic']} "
          f"(velocity {fraud['velocity_threshold']}/{fraud['velocity_window_minutes']}m, "
          f"storm {fraud['storm_threshold']}/{fraud['storm_window_minutes']}m)")
    print("\nChạy `apply` để submit cả hai runner vào Flink.")
    return 0


def _submit(runner_path: str, label: str) -> bool:
    # subprocess (không qua shell) nên đường dẫn container KHÔNG bị MSYS mangle.
    proc = subprocess.run(
        ["docker", "exec", FLINK_CONTAINER, "flink", "run", "-d", "-py", runner_path],
        capture_output=True, text=True,
    )
    out = proc.stdout + proc.stderr
    job_line = [ln for ln in out.splitlines() if "JobID" in ln]
    if proc.returncode == 0 and job_line:
        print(f"  [OK ] {label}: {job_line[0].strip()}")
        return True
    print(f"  [LỖI] {label}:")
    print("        " + "\n        ".join(out.strip().splitlines()[-6:]))
    return False


def cmd_apply() -> int:
    _build_and_write()
    print(f"Submit 2 runner vào {FLINK_CONTAINER} ...")
    ok_metric = _submit(METRIC_RUNNER, "metric_runner")
    ok_fraud = _submit(FRAUD_RUNNER, "fraud_runner")
    print()
    if ok_metric and ok_fraud:
        print("KẾT QUẢ: đã submit cả hai. Kiểm RUNNING bằng `flink list`.")
        return 0
    print("KẾT QUẢ: có runner submit LỖI — xem trên.")
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
