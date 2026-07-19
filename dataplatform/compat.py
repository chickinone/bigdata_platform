"""Compatibility gate — chặn thay đổi contract phá vỡ BACKWARD (Avro/Schema-Registry).

**BACKWARD** = schema MỚI đọc được dữ liệu ghi bằng schema CŨ (consumer nâng cấp
trước producer). Đây là chế độ Schema Registry chặn ở Pha 2 — nay đưa lên PR/CI để
breaking change ĐỎ trước khi merge, thay vì nổ lúc runtime.

Dịch luật Avro BACKWARD sang contract cột (so base ref vs working tree):

  VỠ (chặn merge):
    - Thêm cột `nullable:false` (không default) — reader mới đọc data cũ thiếu cột.
    - Đổi type sang kiểu KHÔNG promote được (vd long->int, string->long).
    - Đổi `nullable: true -> false` (biến optional thành required).
  OK (cho qua):
    - Xoá cột — reader mới bỏ qua field thừa của data cũ (chỉ ghi chú, không chặn).
    - Thêm cột `nullable:true` — reader mới điền null cho data cũ.
    - `nullable: false -> true`; promote type (int->long, string->bytes...).

Type "hiệu dụng" tính theo lớp Avro trên dây: cột `encoded_as: string` (decimal ->
string, ADR-0003) coi là `string`, không phải decimal — vì đó là thứ thật sự ở Avro.
"""
from __future__ import annotations

import subprocess

import yaml

# Promote AN TOÀN cho BACKWARD: reader (schema mới) đọc được writer (kiểu cũ) nếu kiểu
# mới RỘNG hơn. base_type -> tập current_type hợp lệ. Kiểu ngoài bảng phải khớp hệt.
_PROMOTIONS = {
    "int": {"int", "long", "float", "double"},
    "long": {"long", "float", "double"},
    "float": {"float", "double"},
    "double": {"double"},
    "string": {"string", "bytes"},
    "bytes": {"bytes", "string"},
}


def _effective_type(col: dict) -> str:
    """Kiểu ở lớp Avro trên dây. encoded_as:string thắng type logic (decimal->string)."""
    if col.get("encoded_as") == "string":
        return "string"
    return str(col.get("type", "")).strip()


def _nullable(col: dict) -> bool:
    """Thiếu `nullable` = optional (null được) — khớp cách contract đánh dấu cột bắt buộc
    bằng `nullable:false` tường minh."""
    return bool(col.get("nullable", True))


def _type_ok(base_t: str, cur_t: str) -> bool:
    if base_t == cur_t:
        return True
    return cur_t in _PROMOTIONS.get(base_t, set())


def compare_columns(base_cols: list[dict], cur_cols: list[dict]) -> list[str]:
    """Trả danh sách thông điệp VỠ BACKWARD giữa hai bộ cột của cùng một dataset."""
    base = {c["name"]: c for c in base_cols}
    cur = {c["name"]: c for c in cur_cols}
    breaks: list[str] = []

    for name, cc in cur.items():
        bc = base.get(name)
        if bc is None:  # cột MỚI
            if not _nullable(cc):
                breaks.append(f"thêm cột `{name}` nullable=false (không default) — data cũ thiếu cột này")
            continue
        bt, ct = _effective_type(bc), _effective_type(cc)
        if not _type_ok(bt, ct):
            breaks.append(f"cột `{name}`: đổi type `{bt}` -> `{ct}` không promote được (BACKWARD)")
        if _nullable(bc) and not _nullable(cc):
            breaks.append(f"cột `{name}`: đổi nullable true->false (optional thành required)")
    return breaks


def removed_columns(base_cols: list[dict], cur_cols: list[dict]) -> list[str]:
    """Cột bị xoá — BACKWARD cho phép (ghi chú, không chặn)."""
    cur_names = {c["name"] for c in cur_cols}
    return [c["name"] for c in base_cols if c["name"] not in cur_names]


# ---------- đọc contract ở một git ref ----------
def _git(args: list[str]) -> str:
    return subprocess.run(["git", *args], capture_output=True, text=True, encoding="utf-8").stdout


def git_show(ref: str, path: str) -> str | None:
    """Nội dung file ở ref, hoặc None nếu ref đó không có file."""
    proc = subprocess.run(["git", "show", f"{ref}:{path}"], capture_output=True, text=True, encoding="utf-8")
    return proc.stdout if proc.returncode == 0 else None


def git_ls(ref: str, path_prefix: str) -> list[str]:
    """Danh sách file (đường dẫn repo) dưới prefix, tại ref."""
    out = _git(["ls-tree", "-r", "--name-only", ref, "--", path_prefix])
    return [ln for ln in out.splitlines() if ln.strip()]


def datasets_at_ref(ref: str) -> dict[str, dict]:
    """{urn -> raw contract} đọc từ metadata/datasets tại một git ref (KHÔNG validate
    schema — chỉ cần urn + columns để so compat)."""
    out: dict[str, dict] = {}
    for path in git_ls(ref, "metadata/datasets"):
        if not path.endswith(".yaml"):
            continue
        raw = yaml.safe_load(git_show(ref, path) or "")
        if raw and "urn" in raw:
            out[raw["urn"]] = raw
    return out
