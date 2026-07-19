"""Deployer connector Kafka Connect — áp config sinh từ metadata, idempotent.

    python -m dataplatform.deployers.connectors plan    # chỉ xem sẽ đổi gì (mặc định)
    python -m dataplatform.deployers.connectors apply   # PUT thật lên Connect

ĐÂY LÀ CHỖ BIẾN CONTROL PLANE THÀNH "LOAD-BEARING". Trước đây connector đăng ký
bằng `curl -X POST` thủ công — file JSON sinh ra chỉ là tài liệu, thứ THẬT SỰ cấu
hình Connect là bàn tay người. Sau deployer này, "đăng ký connector" = chạy một
lệnh đọc thẳng metadata.

Vì sao `PUT /connectors/{name}/config` chứ không `POST /connectors`:
  - POST tạo mới, gọi lần hai trên connector đã tồn tại -> 409 Conflict.
  - PUT là "đặt config này làm trạng thái hiện tại": chưa có thì tạo, có rồi thì
    cập nhật. Idempotent -> chạy lại bao nhiêu lần cũng an toàn, đúng tinh thần
    plan/apply như Terraform.

Config đọc THẲNG từ generator (không đọc file trên đĩa) nên không thể áp bản cũ:
desired state luôn suy từ contract mới nhất. Các placeholder `${env:...}` giữ
NGUYÊN — Connect tự resolve bằng EnvVarConfigProvider bên trong container, nên
deployer không bao giờ chạm secret.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

from ..generators import debezium, es_sink, s3_sink
from ..registry import ContractError, connections_by_name, load_datasets

# Từ máy host, Connect REST ở localhost:8083 (cổng map trong docker-compose).
# Bên trong mạng compose thì là http://kafka-connect:8083 — override bằng env.
CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083")


def desired_connectors() -> dict[str, dict]:
    """{tên connector -> config} suy THẲNG từ generator.

    Gộp đúng ba generator sinh ra connector. Không đụng dlq/topic/DDL — chúng
    không phải connector. Thêm loại connector mới = thêm một generator vào đây.
    """
    datasets = load_datasets()
    conns = connections_by_name()
    out: dict[str, dict] = {}
    for payload in {
        **es_sink.targets(datasets, conns),
        **s3_sink.targets(datasets, conns),
        **debezium.targets(datasets, conns),
    }.values():
        out[payload["name"]] = payload["config"]
    return dict(sorted(out.items()))


def _req(method: str, path: str, body: dict | None = None) -> tuple[int, dict | str]:
    """Gọi Connect REST. Trả (status_code, payload). Không ném lỗi HTTP — trả về
    để nơi gọi tự quyết, vì 404 (chưa có connector) là trạng thái BÌNH THƯỜNG với
    một deployer, không phải sự cố.
    """
    url = f"{CONNECT_URL}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except json.JSONDecodeError:
            return e.code, raw


def _current_config(name: str) -> dict | None:
    """Config connector đang chạy, hoặc None nếu chưa tồn tại."""
    code, payload = _req("GET", f"/connectors/{name}/config")
    if code == 404:
        return None
    if code == 200 and isinstance(payload, dict):
        return payload
    raise RuntimeError(f"GET config {name} trả {code}: {payload}")


def _diff(desired: dict, current: dict | None) -> tuple[str, list[str]]:
    """So desired với current. Trả (hành_động, danh_sách_khác).

    Chỉ xét các khoá TRONG desired: Connect tự thêm `name` vào config lưu, không
    tính là khác biệt. Nếu mọi khoá desired đã khớp -> UNCHANGED.
    """
    if current is None:
        return "CREATE", []
    diffs = []
    for k, v in desired.items():
        if current.get(k) != v:
            diffs.append(f"~ {k}: {current.get(k)!r} -> {v!r}")
    return ("UPDATE" if diffs else "UNCHANGED"), diffs


def _plan() -> list[tuple[str, str, list[str]]]:
    """Tính kế hoạch mà KHÔNG ghi gì. Đây là 'check' cho deployer."""
    plan = []
    for name, cfg in desired_connectors().items():
        action, diffs = _diff(cfg, _current_config(name))
        plan.append((name, action, diffs))
    return plan


def cmd_plan() -> int:
    print(f"Kế hoạch deploy connector (Connect: {CONNECT_URL}) — KHÔNG ghi gì:\n")
    for name, action, diffs in _plan():
        print(f"  [{action:<9}] {name}")
        for d in diffs:
            print(f"              {d}")
    print("\nChạy `apply` để áp các thay đổi trên.")
    return 0


def _wait_running(names: list[str], attempts: int = 10, delay: float = 2.0) -> dict[str, str]:
    """Đợi connector chuyển RUNNING sau khi apply. Trả {tên -> trạng thái cuối}.

    Có bước này vì PUT trả 200 NGAY khi Connect NHẬN config, chứ chưa chắc task đã
    chạy — snapshot Debezium mất vài giây. Không đợi thì báo 'xong' lúc nó còn có
    thể sắp FAILED.
    """
    result: dict[str, str] = {}
    for _ in range(attempts):
        pending = [n for n in names if result.get(n) not in ("RUNNING", "FAILED")]
        if not pending:
            break
        for name in pending:
            code, payload = _req("GET", f"/connectors/{name}/status")
            if code != 200 or not isinstance(payload, dict):
                result[name] = f"HTTP {code}"
                continue
            cstate = payload.get("connector", {}).get("state", "?")
            tstates = [t.get("state") for t in payload.get("tasks", [])]
            if cstate == "FAILED" or "FAILED" in tstates:
                result[name] = "FAILED"
            elif cstate == "RUNNING" and (not tstates or all(s == "RUNNING" for s in tstates)):
                result[name] = "RUNNING"
            else:
                result[name] = f"{cstate}/tasks={tstates}"
        time.sleep(delay)
    return result


def cmd_apply() -> int:
    plan = _plan()
    changed = [(n, a) for n, a, _ in plan if a in ("CREATE", "UPDATE")]
    if not changed:
        print("Mọi connector đã khớp desired state — không có gì để áp.")
        return 0

    desired = desired_connectors()
    print(f"Áp {len(changed)} connector lên {CONNECT_URL}:\n")
    applied = []
    for name, action in changed:
        code, payload = _req("PUT", f"/connectors/{name}/config", desired[name])
        ok = code in (200, 201)
        print(f"  [{'OK ' if ok else 'LỖI'}] {action:<7} {name}  (HTTP {code})")
        if not ok:
            print(f"          {payload}")
        else:
            applied.append(name)

    print("\nĐợi connector vào RUNNING...")
    states = _wait_running(applied)
    bad = 0
    for name in applied:
        st = states.get(name, "?")
        print(f"  [{'RUNNING ' if st == 'RUNNING' else 'CHƯA/ERR'}] {name}: {st}")
        if st != "RUNNING":
            bad += 1

    print()
    if bad:
        print(f"KẾT QUẢ: {bad}/{len(applied)} connector CHƯA RUNNING — xem status ở trên.")
        return 1
    print(f"KẾT QUẢ: {len(applied)} connector đã áp và RUNNING.")
    return 0


def _force_utf8() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    _force_utf8()
    parser = argparse.ArgumentParser(prog="dataplatform.deployers.connectors")
    parser.add_argument("command", nargs="?", default="plan", choices=["plan", "apply"])
    args = parser.parse_args(argv)
    try:
        return {"plan": cmd_plan, "apply": cmd_apply}[args.command]()
    except ContractError as exc:
        print(f"LỖI CONTRACT\n{exc}", file=sys.stderr)
        return 2
    except urllib.error.URLError as exc:
        print(f"KHÔNG nối được Connect ở {CONNECT_URL}: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
