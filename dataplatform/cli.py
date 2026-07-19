"""CLI của control plane.

    python -m dataplatform.cli check    # so bản sinh với bản đang có trên đĩa
    python -m dataplatform.cli write    # ghi bản sinh đè lên đĩa
    python -m dataplatform.cli show     # in ra để xem, không đụng đĩa

`check` là bước quan trọng nhất của chiến lược strangler-fig: chừng nào bản sinh
còn khác bản viết tay, ta CHƯA được phép cắt chuyển. Nó cũng là lưới an toàn cho
CI về sau — contract sửa mà quên chạy generator thì CI đỏ.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .generators import (
    clickhouse_ddl,
    debezium,
    dlq,
    es_sink,
    lineage,
    postgres_publication,
    s3_sink,
    topic_manifest,
    trino_catalog,
)
from .generators.flink_sql import load_pipelines
from .registry import (
    REPO_ROOT,
    ContractError,
    connections_by_name,
    load_connections,
    load_datasets,
)

# Ghi JSON với indent 2 + newline cuối file. Đây là QUY ƯỚC, không phải yêu cầu
# của Kafka Connect - chọn một kiểu rồi giữ nguyên để diff giữa các lần chạy chỉ
# phản ánh thay đổi thật.
JSON_INDENT = 2


def _serialize(payload) -> str:
    """Biến artifact thành text để ghi ra đĩa.

    Hai loại artifact:
      - dict  -> JSON (connector config, bản kê DLQ...)
      - str   -> text nguyên văn (DDL SQL, publication...)
    """
    if isinstance(payload, str):
        return payload
    return json.dumps(payload, indent=JSON_INDENT, ensure_ascii=False) + "\n"


def _collect() -> dict:
    datasets = load_datasets()
    conns = connections_by_name()
    targets: dict = {}
    targets.update(debezium.targets(datasets, conns))
    targets.update(es_sink.targets(datasets, conns))
    targets.update(s3_sink.targets(datasets, conns))
    targets.update(dlq.targets(datasets))
    targets.update(postgres_publication.targets(datasets))
    targets.update(clickhouse_ddl.targets(datasets))
    targets.update(topic_manifest.targets(datasets, conns))
    targets.update(trino_catalog.targets(load_connections()))
    targets.update(lineage.targets(datasets, load_pipelines()))
    return targets


# Các khoá mà giá trị là DANH SÁCH ngăn bằng dấu phẩy, và thứ tự KHÔNG mang ý
# nghĩa. Kafka Connect coi chúng như một tập hợp — so sánh như chuỗi sẽ báo lệch
# giả chỉ vì generator sắp thứ tự khác người viết tay.
SET_VALUED_KEYS = {"topics", "table.include.list"}


def _normalize(payload: dict) -> dict:
    """Đưa config về dạng so sánh được theo NGỮ NGHĨA.

    Đây là điểm tinh tế của `check`: "so sánh ngữ nghĩa" không chỉ là parse JSON,
    mà còn là biết khoá nào bất biến theo thứ tự.
    """
    out = {"name": payload.get("name"), "config": dict(payload.get("config", {}))}
    for key in SET_VALUED_KEYS:
        if key in out["config"]:
            items = [v.strip() for v in out["config"][key].split(",") if v.strip()]
            out["config"][key] = sorted(items)
    return out


def _compare(rel_path: str, generated) -> tuple[str, list[str]]:
    """So bản sinh với file trên đĩa. Trả về (trạng_thái, danh_sách_khác_biệt).

    Rẽ theo LOẠI artifact:
      - dict (JSON): so NGỮ NGHĨA (parse rồi so dict). File viết tay có dòng trống
        + thứ tự khoá tuỳ người; ép tái tạo từng byte là giòn. Kafka Connect đọc
        JSON, không quan tâm dòng trống.
      - str (SQL/text): so NGUYÊN VĂN. Lý do khác JSON: file này DO CONTROL PLANE
        SỞ HỮU, không công cụ ngoài nào format lại, nên byte-match là hợp lý và
        chặt hơn.
    """
    path = REPO_ROOT / rel_path
    if not path.exists():
        return "MOI", []

    raw = path.read_text(encoding="utf-8")

    if isinstance(generated, str):
        if raw == generated:
            return "KHOP", []
        return "KHAC", _diff_text(raw, generated)

    current = json.loads(raw)
    if _normalize(current) == _normalize(generated):
        return "KHOP", []
    return "KHAC", _diff_keys(_normalize(current), _normalize(generated))


def _diff_text(current: str, generated: str) -> list[str]:
    """Diff dòng cho artifact text — chỉ những dòng thật sự khác."""
    import difflib

    diffs: list[str] = []
    for line in difflib.unified_diff(
        current.splitlines(), generated.splitlines(),
        fromfile="đĩa", tofile="sinh", lineterm="",
    ):
        if line.startswith(("+", "-")) and not line.startswith(("+++", "---")):
            diffs.append(line)
    return diffs[:40]  # đủ để thấy, không tràn màn hình


def _diff_keys(current: dict, generated: dict) -> list[str]:
    """Liệt kê khác biệt ở mức từng khoá config — để đọc được ngay khác chỗ nào,
    thay vì phải tự dò hai khối JSON.
    """
    diffs: list[str] = []
    if current.get("name") != generated.get("name"):
        diffs.append(f"name: {current.get('name')!r} -> {generated.get('name')!r}")

    cur_cfg = current.get("config", {})
    gen_cfg = generated.get("config", {})

    for key in sorted(set(cur_cfg) | set(gen_cfg)):
        old, new = cur_cfg.get(key), gen_cfg.get(key)
        if old == new:
            continue
        if key not in gen_cfg:
            diffs.append(f"- {key}: {old!r}  (bản sinh THIẾU)")
        elif key not in cur_cfg:
            diffs.append(f"+ {key}: {new!r}  (bản sinh THÊM)")
        else:
            diffs.append(f"~ {key}: {old!r} -> {new!r}")
    return diffs


def cmd_check() -> int:
    targets = _collect()
    drift = 0

    print(f"Đối chiếu {len(targets)} artifact sinh từ metadata/ với file trên đĩa:\n")
    for rel_path, payload in sorted(targets.items()):
        status, diffs = _compare(rel_path, payload)
        if status == "KHOP":
            print(f"  [KHỚP] {rel_path}")
        elif status == "MOI":
            print(f"  [MỚI ] {rel_path}  (chưa có trên đĩa)")
            drift += 1
        else:
            print(f"  [KHÁC] {rel_path}")
            for d in diffs:
                print(f"          {d}")
            drift += 1

    print()
    if drift:
        print(f"KẾT QUẢ: {drift}/{len(targets)} artifact lệch.")
        print("Bản sinh CHƯA tái tạo đúng hiện trạng -> chưa được cắt chuyển.")
        return 1

    print(f"KẾT QUẢ: {len(targets)}/{len(targets)} artifact khớp tuyệt đối.")
    print("Contract mang đủ thông tin để sinh lại toàn bộ file viết tay.")
    return 0


def cmd_write() -> int:
    targets = _collect()
    for rel_path, payload in sorted(targets.items()):
        path = REPO_ROOT / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        # newline="\n": ÉP LF kể cả trên Windows. write_text mặc định (newline=None)
        # dịch \n -> \r\n trên Windows, làm hỏng script .sh chạy trong container Linux
        # (set -euo pipefail\r -> option lỗi). Mọi artifact đều cho engine Linux nên
        # đều phải LF. `check` không thấy khác biệt này vì read_text dịch ngược lúc đọc.
        path.write_text(_serialize(payload), encoding="utf-8", newline="\n")
        print(f"  đã ghi  {rel_path}")
    print(f"\nĐã sinh {len(targets)} artifact từ metadata/.")
    return 0


def cmd_show() -> int:
    for rel_path, payload in sorted(_collect().items()):
        print(f"===== {rel_path}")
        print(_serialize(payload))
    return 0


def _force_utf8_output() -> None:
    """Console Windows mặc định là cp1252, không in nổi tiếng Việt và sẽ ném
    UnicodeEncodeError. Ép UTF-8 để công cụ chạy được ở mọi terminal thay vì bắt
    người dùng tự `chcp 65001` trước mỗi lần chạy.
    """
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    _force_utf8_output()
    parser = argparse.ArgumentParser(
        prog="dataplatform.cli",
        description="Sinh artifact vận hành từ dataset contract trong metadata/.",
    )
    parser.add_argument("command", choices=["check", "write", "show"])
    args = parser.parse_args(argv)

    try:
        return {"check": cmd_check, "write": cmd_write, "show": cmd_show}[args.command]()
    except ContractError as exc:
        print(f"LỖI CONTRACT\n{exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
