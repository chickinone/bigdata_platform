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
import os
import sys
from pathlib import Path

from . import compat
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


def _default_base() -> str:
    """Ref nền để so 'plan'/'compat'. CI đặt GITHUB_BASE_REF cho PR; local mặc định main."""
    ref = os.getenv("GITHUB_BASE_REF")
    return f"origin/{ref}" if ref else "origin/main"


def _compare_against(base_text: str, generated) -> tuple[str, list[str]]:
    """Như _compare nhưng so với NỘI DUNG ở ref nền (không phải file trên đĩa).
    Hướng diff: nền -> bản sinh từ metadata hiện tại."""
    if isinstance(generated, str):
        if base_text == generated:
            return "SAME", []
        return "ĐỔI", _diff_text(base_text, generated)
    base_obj = json.loads(base_text)
    if _normalize(base_obj) == _normalize(generated):
        return "SAME", []
    return "ĐỔI", _diff_keys(_normalize(base_obj), _normalize(generated))


def cmd_plan(base: str) -> int:
    """'terraform plan' cho metadata: artifact NÀO sẽ đổi khi merge PR vào `base`.

    So bản sinh từ metadata HIỆN TẠI với artifact đã commit ở `base` — reviewer thấy
    HỆ QUẢ vận hành của một thay đổi contract, không chỉ diff YAML. Thuần tĩnh + git,
    không cần engine. Informational (exit 0)."""
    targets = _collect()
    new: list[str] = []
    changed: list[tuple[str, list[str]]] = []
    for rel, payload in sorted(targets.items()):
        base_text = compat.git_show(base, rel)
        if base_text is None:
            new.append(rel)
            continue
        status, diffs = _compare_against(base_text, payload)
        if status == "ĐỔI":
            changed.append((rel, diffs))

    print(f"PLAN vs `{base}` — hệ quả khi merge:\n")
    for rel in new:
        print(f"  [MỚI ] {rel}")
    for rel, diffs in changed:
        print(f"  [ĐỔI ] {rel}")
        for d in diffs:
            print(f"          {d}")
    if not new and not changed:
        print("  (không artifact nào đổi)")
    print(f"\nKẾT QUẢ: {len(new) + len(changed)} artifact đổi — {len(new)} mới, {len(changed)} sửa.")
    return 0


def cmd_compat(base: str) -> int:
    """Gate BACKWARD: chặn thay đổi contract phá tương thích ngược (xem compat.py).
    So dataset ở `base` với working tree. Exit 1 nếu có breaking change."""
    base_ds = compat.datasets_at_ref(base)
    if not base_ds:
        print(f"Không đọc được dataset ở '{base}' (ref mới/nông?) — bỏ qua gate.")
        return 0
    cur_ds = {d.urn: d.raw for d in load_datasets()}

    breaks = 0
    print(f"COMPAT (BACKWARD) vs `{base}`:\n")
    for urn, cur in sorted(cur_ds.items()):
        base_raw = base_ds.get(urn)
        if base_raw is None:
            continue  # dataset mới — không có gì để phá
        base_cols, cur_cols = base_raw.get("columns", []), cur.get("columns", [])
        msgs = compat.compare_columns(base_cols, cur_cols)
        removed = compat.removed_columns(base_cols, cur_cols)
        if msgs:
            print(f"  [VỠ] {urn}")
            for m in msgs:
                print(f"        {m}")
            breaks += len(msgs)
        if removed:
            print(f"  [note] {urn}: xoá cột {removed} — BACKWARD cho phép, kiểm consumer.")
    for urn in sorted(set(base_ds) - set(cur_ds)):
        print(f"  [note] xoá dataset `{urn}` — kiểm consumer hạ nguồn.")

    print()
    if breaks:
        print(f"KẾT QUẢ: {breaks} thay đổi PHÁ BACKWARD -> chặn merge.")
        return 1
    print("KẾT QUẢ: không có thay đổi phá BACKWARD.")
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
    parser.add_argument("command", choices=["check", "write", "show", "plan", "compat"])
    parser.add_argument("--base", default=None,
                        help="Git ref nền để so 'plan'/'compat' (mặc định origin/main hoặc GITHUB_BASE_REF).")
    args = parser.parse_args(argv)

    base = args.base or _default_base()
    try:
        if args.command == "plan":
            return cmd_plan(base)
        if args.command == "compat":
            return cmd_compat(base)
        return {"check": cmd_check, "write": cmd_write, "show": cmd_show}[args.command]()
    except ContractError as exc:
        print(f"LỖI CONTRACT\n{exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
