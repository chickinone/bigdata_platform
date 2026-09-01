import importlib

import pytest

from dataplatform.cli import APPLY_STEPS, OM_STEP, VERIFIERS

ALL_STEPS = APPLY_STEPS + [OM_STEP]


def test_moi_buoc_tro_toi_module_co_that():
    """Buoc phai goi duoc — neu khong `cli apply` se no giua chung, sau khi da ap
    nhung buoc truoc do. That bai nua chung tren engine that la dieu dat nhat."""
    for st in ALL_STEPS:
        mod = importlib.import_module(st["module"])
        assert hasattr(mod, "main"), f"{st['module']} thieu main()"


def test_ten_buoc_duy_nhat():
    names = [st["name"] for st in ALL_STEPS]
    assert len(names) == len(set(names))


@pytest.mark.parametrize("st", ALL_STEPS, ids=lambda s: s["name"])
def test_needs_tro_toi_verifier_co_that(st):
    """`needs` phai la ten verifier CO THAT. Go sai ten thi gate im lang khong bao gio
    chay — va ta lai co dung cai "bo do ton tai nhung khong ai goi" cua ADR-0043."""
    if st.get("needs") is not None:
        assert st["needs"] in VERIFIERS, f"{st['name']}.needs={st['needs']!r} khong co trong VERIFIERS"


def test_flink_phai_doi_bang_clickhouse():
    """Rang buoc NGHIEP VU, khong phai hinh thuc.

    Flink metric runner ghi vao bang dich ClickHouse. Bang khong ton tai thi job VAN
    submit thanh cong roi im lang khong ghi duoc — dung kieu hong da xay ra khi dung
    lanh (ADR-0043), va khong service nao bao loi. Gate nay la thu duy nhat chan no.
    Neu ai do go `needs` di, test nay phai do.
    """
    flink = next(st for st in APPLY_STEPS if st["name"] == "flink")
    assert flink["needs"] == "clickhouse_schema"


def test_thu_tu_clickhouse_truoc_flink():
    names = [st["name"] for st in APPLY_STEPS]
    assert names.index("clickhouse") < names.index("flink")


def test_connectors_chay_dau_tien():
    """Khong co CDC thi khong co gi chay trong topic, nen moi buoc sau deu vo nghia."""
    assert APPLY_STEPS[0]["name"] == "connectors"


def test_openmetadata_khong_nam_trong_chuoi_mac_dinh():
    """OM la phien rieng: can dung stack chinh de nhuong RAM va can ES cua OM song.
    De no vao chuoi mac dinh se lam `cli apply` do o may khong bat OM."""
    assert OM_STEP not in APPLY_STEPS
