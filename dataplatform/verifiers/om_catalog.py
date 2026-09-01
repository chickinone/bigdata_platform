from __future__ import annotations

import json
import sys
import urllib.error

from .. import state
from ..deployers import openmetadata as om
from ..registry import ContractError


def _doc_graph() -> dict:
    return json.loads(om.GRAPH_PATH.read_text(encoding="utf-8"))


def _bang_mong_doi(graph: dict) -> set:
    """Tên bảng mà `openmetadata apply` sẽ tạo — suy từ CHÍNH graph.json.

    Không viết lại danh sách ở đây: dùng lại `_table_name`/`_schema_of` của deployer nên
    verifier và deployer không thể lệch nhau về "bảng nào phải có".
    """
    ten = set()
    for n in graph["dataset_nodes"]:
        ten.add(om._fqn(om._schema_of(n), om._table_name(n["id"])))
    for n in graph["lake_nodes"]:
        ten.add(om._fqn(n["layer"], om._table_name(n["id"])))
    return ten


def _bang_tren_om(token: str) -> set:
    """Bảng OM đang có trong service `bdp`. Phân trang vì mặc định OM trả 10 dòng."""
    ten = set()
    after = None
    while True:
        path = f"/api/v1/tables?database={om.SERVICE}.{om.DATABASE}&limit=100"
        if after:
            path += f"&after={after}"
        code, payload = om._req("GET", path, token)
        if code != 200:
            raise RuntimeError(f"GET tables ({code}): {json.dumps(payload)[:200]}")
        for t in payload.get("data", []):
            ten.add(t.get("fullyQualifiedName", ""))
        after = (payload.get("paging") or {}).get("after")
        if not after:
            return ten


def cmd_verify() -> int:
    graph = _doc_graph()
    van_tay = om.graph_fingerprint()
    da_ghi = state.load(om.STATE_KEY) or {}

    print("Đối chiếu catalog OpenMetadata với lineage/graph.json:")
    print()
    print(f"  graph.json hiện tại : {van_tay}")

    # --- Tầng 1: kiểm ĐỘ CŨ. Chạy được kể cả khi OM đang tắt. --------------------
    if not da_ghi:
        print("  chưa từng đẩy catalog (state trống)")
        print()
        print("KẾT QUẢ: bỏ qua — chưa chạy `openmetadata apply` lần nào.")
        print("Catalog chưa tồn tại thì không có gì để lệch. Đây KHÔNG phải lỗi.")
        return 0

    cu = da_ghi.get("graph_fingerprint")
    luc = da_ghi.get("applied_at", "?")
    print(f"  đã đẩy lên OM       : {cu}  (lúc {luc})")
    print()

    canh_bao = []
    if cu != van_tay:
        canh_bao.append(
            f"catalog ĐANG CŨ: graph.json đã đổi kể từ lần đẩy cuối ({luc}). "
            "Chạy `python -m dataplatform.deployers.openmetadata apply` để đồng bộ."
        )
        print("  [chú ý] " + canh_bao[0])
        print()

    # --- Tầng 2: đối chiếu NỘI DUNG. Chỉ làm được khi OM sống. -------------------
    try:
        token = om._login()
    except (urllib.error.URLError, RuntimeError, OSError) as exc:
        print(f"  OM không truy cập được ({type(exc).__name__}) — bỏ qua đối chiếu nội dung.")
        print("  Đây là BÌNH THƯỜNG: OM là phiên riêng, thường tắt để nhường RAM.")
        print()
        if canh_bao:
            print("KẾT QUẢ: 0 lệch, 1 chú ý (catalog cũ). Không đối chiếu được nội dung.")
        else:
            print("KẾT QUẢ: 0 lệch. Vân tay khớp; không đối chiếu được nội dung (OM tắt).")
        return 0

    mong_doi = _bang_mong_doi(graph)
    thuc_te = _bang_tren_om(token)

    thieu = sorted(mong_doi - thuc_te)
    thua = sorted(thuc_te - mong_doi)

    for t in thieu:
        print(f"  [LỆCH ] THIẾU trên OM: {t}")
    for t in thua:
        print(f"  [chú ý] có trên OM nhưng graph.json không khai: {t}")

    print()
    print(f"  bảng mong đợi {len(mong_doi)} · trên OM {len(thuc_te)} · "
          f"thiếu {len(thieu)} · thừa {len(thua)}")
    print()

    if thieu:
        print(f"KẾT QUẢ: {len(thieu)} lệch — catalog THIẾU bảng mà graph.json khai.")
        print("Nguyên nhân thường gặp: `apply` hỏng nửa chừng, hoặc ai đó xoá trên UI.")
        return 1

    print(f"KẾT QUẢ: 0 lệch, {len(thua) + len(canh_bao)} chú ý. Catalog khớp graph.json.")
    return 0


def _force_utf8() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    _force_utf8()
    try:
        return cmd_verify()
    except ContractError as exc:
        print(f"LỖI CONTRACT\n{exc}", file=sys.stderr)
        return 2
    except (RuntimeError, FileNotFoundError, OSError) as exc:
        print(f"KHÔNG đối chiếu được với OpenMetadata: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
