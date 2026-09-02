from pathlib import Path

from dataplatform.cli import VERIFIERS
from dataplatform.verifiers import connect_health as ch

REPO = Path(__file__).resolve().parents[1]
NGUON = (REPO / "dataplatform/verifiers/connect_health.py").read_text(encoding="utf-8")


def test_nam_trong_vong_lap_verify():
    assert "connect_health" in VERIFIERS


def test_chi_RUNNING_moi_la_khoe():
    """PAUSED phải bị coi là lệch. Connector tạm dừng trông 'không lỗi' nhưng không tiêu
    thụ gì — im lặng đúng kiểu verifier này sinh ra để bắt."""
    assert ch.KHOE == "RUNNING"
    assert "PAUSED" not in ch.KHOE


def test_xet_TASK_state_khong_chi_connector_state():
    """Lý do verifier này tồn tại.

    Kafka Connect báo connector=RUNNING trong khi task=FAILED. Ai chỉ nhìn
    `connector.state` sẽ thấy mọi thứ bình thường — CDC đã chết hơn một ngày theo đúng
    cách đó, và 4/5 ES sink chết trong lúc test tải mà không gì báo.
    """
    assert "tstates" in NGUON
    assert 'payload.get("tasks", [])' in NGUON
    # Không được kết luận khoẻ chỉ từ cstate.
    assert "xau = [s for s in tstates if s != KHOE]" in NGUON


def test_task_rong_cung_la_loi():
    """Connector RUNNING mà 0 task = không ai làm việc. Trông sạch, chạy rỗng."""
    assert "if not tstates:" in NGUON
    assert "0 task" in NGUON


def test_dung_desired_connectors_cua_deployer():
    """Không được giữ bản sao danh sách connector: lệch nhau về 'connector nào phải có'
    là đúng thứ sprawl cả dự án đang diệt."""
    assert "dep.desired_connectors()" in NGUON
    assert "dep._req" in NGUON


def test_connect_chet_khong_lam_cong_do():
    """Connect không nối được = exit 3 (chưa kết luận được), KHÔNG phải exit 1 (lệch).
    Gộp hai loại sẽ biến 'stack chưa bật' thành báo động giả, mà báo động giả lặp lại
    thì cả cổng bị bỏ qua (ADR-0041/0043)."""
    duoi = NGUON[NGUON.find("def main("):]
    assert "URLError" in duoi
    assert "return 3" in duoi


def test_connector_thua_chi_la_chu_y():
    """Connector lạ trên Connect không làm verifier đỏ — GC của `connectors apply` mới
    là chỗ xử lý nó (ADR-0045)."""
    assert "chu_y.append" in NGUON
    vi_tri = NGUON.find("dang_song - set(mong_doi)")
    assert vi_tri > 0
    assert "loi.append" not in NGUON[vi_tri:vi_tri + 300]


def test_chi_doc_khong_ghi():
    """Verifier chỉ ĐỌC. Có ghi là nó thành deployer thứ hai."""
    for ghi in ('"PUT"', '"POST"', '"DELETE"', '"PATCH"'):
        assert ghi not in NGUON, f"verifier không được ghi: thấy {ghi}"


def test_dong_goc_lay_caused_by_sau_nhat():
    """Kafka Connect bọc lỗi nhiều tầng; dòng đầu luôn là 'Tolerance exceeded in error
    handler' — vô nghĩa. Nguyên nhân thật ở `Caused by` cuối cùng."""
    trace = (
        "org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded\n"
        "\tat org.apache.kafka.connect.runtime.WorkerTask.run(WorkerTask.java:259)\n"
        "Caused by: org.apache.kafka.connect.errors.ConnectException: Bulk request failed\n"
        "Caused by: java.net.ConnectException: Connection refused\n"
    )
    assert "Connection refused" in ch._dong_goc(trace)
    assert "Tolerance exceeded" not in ch._dong_goc(trace)


def test_dong_goc_chiu_duoc_trace_rong():
    assert ch._dong_goc("") == ""
    assert ch._dong_goc("chỉ một dòng") == "chỉ một dòng"
