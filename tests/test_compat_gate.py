from __future__ import annotations

from dataplatform.compat import _effective_type, compare_columns, removed_columns


def _c(name: str, type_: str, **kw) -> dict:
    return {"name": name, "type": type_, **kw}


# ---------- kiểu hiệu dụng ở lớp Avro ----------
def test_encoded_as_thang_kieu_logic():
    # decimal + encoded_as:string -> trên dây là string (ADR-0003). Gate phải so ở lớp
    # THẬT trên Kafka, không phải kiểu nghiệp vụ.
    assert _effective_type(_c("amount", "decimal(19,4)", encoded_as="string")) == "string"
    assert _effective_type(_c("account_id", "long")) == "long"


# ---------- VỠ BACKWARD ----------
def test_them_cot_required_la_vo():
    base = [_c("a", "long")]
    cur = [_c("a", "long"), _c("b", "string", nullable=False)]
    assert compare_columns(base, cur), "thêm cột nullable=false phải là vỡ"


def test_thu_hep_type_la_vo():
    assert compare_columns([_c("a", "long")], [_c("a", "int")])


def test_optional_thanh_required_la_vo():
    base = [_c("a", "string", nullable=True)]
    cur = [_c("a", "string", nullable=False)]
    assert compare_columns(base, cur)


# ---------- KHÔNG vỡ ----------
def test_them_cot_nullable_thi_qua():
    base = [_c("a", "long")]
    cur = [_c("a", "long"), _c("b", "string")]
    assert compare_columns(base, cur) == []


def test_promote_type_thi_qua():
    assert compare_columns([_c("a", "int")], [_c("a", "long")]) == []
    assert compare_columns([_c("a", "float")], [_c("a", "double")]) == []


def test_required_thanh_optional_thi_qua():
    base = [_c("a", "string", nullable=False)]
    cur = [_c("a", "string", nullable=True)]
    assert compare_columns(base, cur) == []


def test_thieu_nullable_coi_la_optional():
    # Contract đánh dấu cột bắt buộc bằng `nullable: false` tường minh, nên vắng mặt
    # phải hiểu là optional — nếu hiểu ngược, mọi cột đều thành required và gate đỏ oan.
    base = [_c("a", "string")]
    cur = [_c("a", "string"), _c("b", "string")]
    assert compare_columns(base, cur) == []


# ---------- xoá cột: cho qua nhưng phải kê ----------
def test_xoa_cot_khong_vo_nhung_duoc_kê():
    base = [_c("a", "long"), _c("b", "string")]
    cur = [_c("a", "long")]
    assert compare_columns(base, cur) == []
    assert removed_columns(base, cur) == ["b"]
