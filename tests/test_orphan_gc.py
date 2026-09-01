from pathlib import Path

import pytest

from dataplatform.cli import OWNED_GLOBS, _collect, _orphan_files
from dataplatform.deployers import connectors

REPO = Path(__file__).resolve().parents[1]


def test_khong_co_file_thua_o_hien_trang():
    """Hien trang phai sach: moi file trong vung control plane so huu deu duoc sinh."""
    assert _orphan_files(_collect()) == []


def test_owned_globs_deu_tro_toi_thu_muc_co_that():
    for glob in OWNED_GLOBS:
        thu_muc = REPO / Path(glob).parent
        assert thu_muc.is_dir(), f"{glob} tro toi thu muc khong ton tai"


def test_moi_owned_glob_deu_dang_sinh_it_nhat_mot_file():
    """Glob khong khop artifact nao = da khai sai duong dan, va khi do GC se khong bao
    gio phat hien duoc file thua trong vung do — im lang mat tac dung."""
    targets = set(_collect())
    for glob in OWNED_GLOBS:
        khop = [t for t in targets if Path(t).match(glob)]
        assert khop, f"khong artifact nao khop {glob}"


def test_phat_hien_file_thua(tmp_path, monkeypatch):
    """Them mot file la vao vung so huu thi phai bi goi la thua."""
    targets = _collect()
    mau = next(t for t in targets if t.startswith("kafka-connect/es-sinks/"))
    gia = REPO / "kafka-connect/es-sinks/es-sink-__test-thua__.json"
    gia.write_text((REPO / mau).read_text(encoding="utf-8"), encoding="utf-8")
    try:
        thua = _orphan_files(targets)
        assert "kafka-connect/es-sinks/es-sink-__test-thua__.json" in thua
    finally:
        gia.unlink()


def test_state_chi_chua_connector():
    """Cau truc state phai on dinh — doc bang key co dinh o nhieu cho."""
    assert connectors.STATE_PATH.name == ".platform-state.json"


def test_state_khong_duoc_commit():
    """State la per-environment (giong Terraform). Commit vao se lam may khac tuong
    minh da tao nhung thu chua he tao — va lan apply sau se XOA nham."""
    gitignore = (REPO / ".gitignore").read_text(encoding="utf-8")
    assert ".platform-state.json" in gitignore


@pytest.mark.parametrize("ten_ham", ["_prune", "_load_state", "_save_state"])
def test_connectors_co_du_ham_gc(ten_ham):
    assert hasattr(connectors, ten_ham)


def test_ranh_gioi_an_toan_khong_xoa_thu_mang_du_lieu():
    """Rang buoc THIET KE, khong phai hinh thuc.

    Chi duoc tu dong xoa nhung thu tao lai tu metadata la co ngay: config connector va
    file sinh. Topic Kafka / bang ClickHouse / duong dan S3 mang DU LIEU — xoa la mat
    khong hoi phuc duoc, nen chi duoc BAO CAO. Test nay chan viec ai do sau nay them
    DROP TABLE hay delete topic vao duong GC.
    """
    nguon = (REPO / "dataplatform/deployers/connectors.py").read_text(encoding="utf-8")
    for nguy_hiem in ("DROP TABLE", "--delete", "delete_topics", "DROP DATABASE"):
        assert nguy_hiem not in nguon, f"duong GC khong duoc chua {nguy_hiem!r}"

    ch = (REPO / "dataplatform/verifiers/clickhouse_schema.py").read_text(encoding="utf-8")
    # Verifier duoc phep NHAC toi DROP TABLE trong thong diep huong dan, nhung khong
    # duoc CHAY no: _ch_query chi dung cho SELECT.
    assert "_ch_query(\"SELECT" in ch or "_ch_query('SELECT" in ch
