from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request

from ..generators import flink_sql
from .. import state
from ..registry import REPO_ROOT, ContractError, endpoint, connections_by_name

# Endpoint nội bộ mạng compose (job chạy TRONG container Flink) — nay đọc từ connection
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


STATE_KEY = "flink_jobs"
FLINK_REST = os.getenv("FLINK_REST", "http://localhost:8082")


def _job_id(out: str) -> str:
    """JobID trong dòng 'Job has been submitted with JobID <32 hex>'."""
    m = re.search(r"JobID\s+([0-9a-f]{32})", out)
    return m.group(1) if m else ""


def _dang_chay() -> dict:
    """{jobid: state} của mọi job Flink biết được. Rỗng nếu không hỏi được REST."""
    try:
        with urllib.request.urlopen(f"{FLINK_REST}/jobs/overview", timeout=10) as r:
            data = json.loads(r.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError):
        return {}
    return {j["jid"]: j.get("state", "?") for j in data.get("jobs", [])}


def _huy_job_cu() -> list:
    """Huỷ job của lần apply TRƯỚC trước khi submit bản mới.

    Không có bước này thì `apply` là CỘNG THÊM chứ không phải THAY THẾ: submit lại sẽ
    để hai bản metric_runner cùng đọc một topic và cùng ghi vào một sink ClickHouse —
    metric bị nhân đôi mà không có lỗi nào. Đã xảy ra thật 02/09.

    Chỉ huỷ job ĐÃ GHI TRONG STATE — cùng lý do với GC connector (ADR-0045): so thẳng
    với danh sách đang chạy sẽ huỷ nhầm job người khác submit tay để thử.
    """
    cu = state.load(STATE_KEY, {}) or {}
    if not cu:
        return []
    song = _dang_chay()
    da_huy = []
    for label, jid in sorted(cu.items()):
        if song.get(jid) not in ("RUNNING", "RESTARTING", "CREATED", "INITIALIZING"):
            continue
        proc = subprocess.run(
            ["docker", "exec", FLINK_CONTAINER, "flink", "cancel", jid],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        ok = proc.returncode == 0
        print(f"  [{'OK ' if ok else 'LỖI'}] huỷ job cũ {label}: {jid[:8]}")
        if ok:
            da_huy.append(label)
    return da_huy


def _submit(runner_path: str, label: str) -> bool:
    # subprocess (không qua shell) nên đường dẫn container không bị MSYS mangle.
    proc = subprocess.run(
        ["docker", "exec", FLINK_CONTAINER, "flink", "run", "-d", "-py", runner_path],
        capture_output=True, text=True,
    )
    out = proc.stdout + proc.stderr
    job_line = [ln for ln in out.splitlines() if "JobID" in ln]
    if proc.returncode == 0 and job_line:
        print(f"  [OK ] {label}: {job_line[0].strip()}")
        return _job_id(out) or True
    print(f"  [LỖI] {label}:")
    print("        " + "\n        ".join(out.strip().splitlines()[-6:]))
    return False


def cmd_apply() -> int:
    _build_and_write()

    # THAY THẾ, không CỘNG THÊM: huỷ job của lần apply trước rồi mới submit bản mới.
    da_huy = _huy_job_cu()

    print(f"Submit 2 runner vào {FLINK_CONTAINER} ...")
    ok_metric = _submit(METRIC_RUNNER, "metric_runner")
    ok_fraud = _submit(FRAUD_RUNNER, "fraud_runner")

    # Ghi state SAU khi submit: phản ánh job THẬT SỰ đang chạy, không phải job định chạy.
    moi = {}
    if isinstance(ok_metric, str):
        moi["metric_runner"] = ok_metric
    if isinstance(ok_fraud, str):
        moi["fraud_runner"] = ok_fraud
    if moi:
        state.save(STATE_KEY, moi)

    print()
    if ok_metric and ok_fraud:
        tail = f", huỷ {len(da_huy)} job cũ" if da_huy else ""
        print(f"KẾT QUẢ: đã submit cả hai{tail}. "
              "Kiểm bằng `python -m dataplatform.verifiers.flink_jobs`.")
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
