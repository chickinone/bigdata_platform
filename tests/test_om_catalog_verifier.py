import json
from pathlib import Path

from dataplatform.cli import VERIFIERS
from dataplatform.deployers import openmetadata as om
from dataplatform.verifiers import om_catalog

REPO = Path(__file__).resolve().parents[1]


def test_nam_trong_vong_lap_verify():
    """Không nằm trong VERIFIERS thì `cli verify` không gọi — và ta lại có thêm một
    'bộ dò tồn tại nhưng không ai gọi' (ADR-0043). OM đã chết 8 ngày mà không ai biết
    đúng vì nó nằm ngoài mọi vòng lặp."""
    assert "om_catalog" in VERIFIERS


def test_van_tay_on_dinh_va_theo_noi_dung():
    a = om.graph_fingerprint()
    assert a == om.graph_fingerprint(), "vân tay phải ổn định giữa hai lần đọc"
    assert len(a) == 16


def test_bang_mong_doi_suy_tu_chinh_graph():
    """Verifier KHÔNG được giữ bản sao danh sách bảng: nó phải dùng lại hàm của deployer,
    nếu không hai bên sẽ lệch nhau về 'bảng nào phải có' — đúng loại sprawl dự án diệt."""
    graph = json.loads(om.GRAPH_PATH.read_text(encoding="utf-8"))
    ten = om_catalog._bang_mong_doi(graph)
    assert ten, "graph.json phải sinh ra ít nhất một bảng"
    assert len(ten) == len(graph["dataset_nodes"]) + len(graph["lake_nodes"])
    for t in ten:
        assert t.startswith(f"{om.SERVICE}.{om.DATABASE}."), f"FQN sai khuôn: {t}"


def test_dung_ham_cua_deployer_khong_chep_lai():
    nguon = (REPO / "dataplatform/verifiers/om_catalog.py").read_text(encoding="utf-8")
    for ham in ("om._fqn", "om._schema_of", "om._table_name", "om.GRAPH_PATH"):
        assert ham in nguon, f"phải dùng lại {ham} của deployer, không tự viết lại"


def test_om_tat_khong_lam_cong_do():
    """Ràng buộc THIẾT KẾ, không phải hình thức.

    OM là phiên riêng — bình thường nó TẮT. Nếu verifier trả exit 1 (lệch) khi không nối
    được, `cli verify` sẽ đỏ mỗi giờ, và một cổng luôn đỏ là một cổng bị bỏ qua (cùng lý
    do đã chọn --no-git cho gitleaks, ADR-0041). Đường 'không nối được' phải trả 0.
    """
    nguon = (REPO / "dataplatform/verifiers/om_catalog.py").read_text(encoding="utf-8")
    vi_tri = nguon.find("OM không truy cập được")
    assert vi_tri > 0, "phải xử lý riêng trường hợp OM tắt"
    sau_do = nguon[vi_tri:vi_tri + 700]
    assert "return 0" in sau_do, "OM tắt phải trả 0, không phải 1"
    assert "return 1" not in sau_do.split("return 0")[0]


def test_chi_thieu_bang_moi_la_lech():
    """Bảng THỪA trên OM chỉ là chú ý: OM có thể chứa entity của project khác trên cùng
    instance. Chỉ THIẾU bảng mới là lệch thật — apply hỏng hoặc ai đó xoá trên UI."""
    nguon = (REPO / "dataplatform/verifiers/om_catalog.py").read_text(encoding="utf-8")
    assert "if thieu:" in nguon
    assert "if thua:" not in nguon, "bảng thừa không được làm verifier đỏ"


def test_khong_ghi_gi_len_om():
    """Verifier chỉ ĐỌC. Có ghi là nó thành deployer thứ hai, và hai nơi cùng đẩy lên OM
    thì không còn biết trạng thái nào là đúng."""
    nguon = (REPO / "dataplatform/verifiers/om_catalog.py").read_text(encoding="utf-8")
    for ghi in ('om._put', 'om._patch', '"PUT"', '"POST"', '"DELETE"', '"PATCH"'):
        assert ghi not in nguon, f"verifier không được ghi: thấy {ghi}"
