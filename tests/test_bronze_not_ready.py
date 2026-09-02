from pathlib import Path

from dataplatform.deployers import spark_batch as sb

REPO = Path(__file__).resolve().parents[1]
RUNNER = (REPO / "spark/jobs/medallion_runner.py").read_text(encoding="utf-8")
DEP = (REPO / "dataplatform/deployers/spark_batch.py").read_text(encoding="utf-8")


def test_ma_not_ready_khac_ma_loi():
    """Phải là mã RIÊNG. Gộp 'chưa có dữ liệu' vào mã lỗi sẽ làm `cli apply` đỏ mỗi lần
    dựng lạnh — mà một cổng luôn đỏ là một cổng bị bỏ qua (ADR-0041)."""
    assert sb.NOT_READY == 4
    assert sb.NOT_READY not in (0, 1, 2, 3)


def test_runner_bat_dung_ba_dang_loi_duong_dan():
    """Spark báo path trống bằng nhiều thông điệp khác nhau tuỳ phiên bản/nguồn."""
    for m in ("Path does not exist", "UNABLE_TO_INFER_SCHEMA", "PATH_NOT_FOUND"):
        assert m in RUNNER


def test_runner_khong_nuot_loi_that():
    """Chỉ lỗi 'không có dữ liệu' mới thành mã 4; lỗi khác phải ném lại nguyên vẹn."""
    assert "raise" in RUNNER.split("SystemExit(4)")[1][:200]


def test_not_ready_khong_bi_coi_la_that_bai():
    """`not 4` là False nên nhánh lỗi không bắt nhầm; và True == 4 là False nên nhánh
    not-ready không bắt nhầm lần chạy thành công."""
    assert "kq is not True and kq == NOT_READY" in DEP
    assert (True == sb.NOT_READY) is False
    assert bool(sb.NOT_READY) is True


def test_chua_san_sang_tra_ve_0():
    """Bronze trống trên stack vừa dựng là BÌNH THƯỜNG, không phải lỗi."""
    vt = DEP.find("if chua_san_sang:")
    assert vt > 0
    assert "return 0" in DEP[vt:vt + 400]


def test_van_con_duong_bao_loi_that():
    assert "if failed:" in DEP and "return 1" in DEP
