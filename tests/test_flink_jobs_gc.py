from pathlib import Path

from dataplatform.cli import VERIFIERS
from dataplatform.deployers import flink_metrics as fm
from dataplatform.verifiers import flink_jobs as fj

REPO = Path(__file__).resolve().parents[1]
DEP = (REPO / "dataplatform/deployers/flink_metrics.py").read_text(encoding="utf-8")
VER = (REPO / "dataplatform/verifiers/flink_jobs.py").read_text(encoding="utf-8")
FRAUD = (REPO / "flink/jobs/fraud_runner.py").read_text(encoding="utf-8")


def test_verifier_nam_trong_vong_lap():
    assert "flink_jobs" in VERIFIERS


def test_apply_la_THAY_THE_khong_phai_cong_them():
    """Không huỷ job cũ thì submit lại tạo bản thứ hai, hai job cùng ghi một sink
    ClickHouse -> metric nhân đôi mà không lỗi nào. Đã xảy ra thật 02/09."""
    assert "_huy_job_cu()" in DEP
    vt_huy = DEP.find("da_huy = _huy_job_cu()")
    vt_submit = DEP.find('_submit(METRIC_RUNNER')
    assert 0 < vt_huy < vt_submit, "phải huỷ TRƯỚC khi submit"


def test_chi_huy_job_trong_state():
    """So thẳng với danh sách đang chạy sẽ huỷ nhầm job người khác submit tay —
    cùng lý do đã chọn state cho GC connector (ADR-0045)."""
    assert "state.load(STATE_KEY" in DEP


def test_ghi_state_sau_khi_submit():
    vt_submit = DEP.find('_submit(FRAUD_RUNNER')
    vt_save = DEP.find("state.save(STATE_KEY")
    assert 0 < vt_submit < vt_save, "state phải phản ánh job THẬT SỰ chạy"


def test_job_id_parse_dung():
    assert fm._job_id("Job has been submitted with JobID 632c7360545ae58f46f64da1399dca63") \
        == "632c7360545ae58f46f64da1399dca63"
    assert fm._job_id("khong co gi") == ""


def test_verifier_doc_lich_su_exception_khong_chi_state():
    """`flink list` chỉ hiện state TỨC THỜI: job chết-rồi-restart vẫn hiện RUNNING mỗi
    lần nhìn. on_timer trả None đã ẩn 1.104 lỗi theo đúng cách đó. Lịch sử exception
    cộng dồn nên không nói dối."""
    assert "exceptionHistory" in VER
    assert "_exception_gan_nhat" in VER
    assert "RUNNING nhưng đã hỏng" in VER


def test_dang_len_khong_bi_coi_la_hong():
    """Job vừa submit chưa kịp RUNNING không phải lỗi."""
    for s in ("CREATED", "INITIALIZING", "RESTARTING"):
        assert s in fj.DANG_LEN


def test_flink_chet_tra_ma_3_khong_phai_1():
    duoi = VER[VER.find("def main("):]
    assert "URLError" in duoi and "return 3" in duoi


def test_verifier_chi_doc():
    for ghi in ('"POST"', '"DELETE"', '"PATCH"', "flink cancel", "flink run"):
        assert ghi not in VER, f"verifier không được ghi: {ghi}"


def test_on_timer_tra_ve_iterable():
    """PyFlink gọi `yield from on_timer(...)`. Hàm không yield và không return iterable
    sẽ trả None -> TypeError: 'NoneType' object is not iterable."""
    vt = FRAUD.find("def on_timer")
    assert vt > 0
    than = FRAUD[vt:vt + 1400]
    assert "return []" in than or "yield" in than


def test_get_state_rong_khong_no():
    """`ListState.get()` trả None khi rỗng — `list(None)` cũng nổ TypeError."""
    assert FRAUD.count("self.failed_history.get() or []") == 2
