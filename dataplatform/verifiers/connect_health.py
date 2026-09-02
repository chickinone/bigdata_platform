from __future__ import annotations

import json
import sys
import urllib.error

from ..deployers import connectors as dep
from ..registry import ContractError

# Trạng thái duy nhất được coi là khoẻ. PAUSED cố ý KHÔNG nằm đây: connector tạm dừng
# trông "không lỗi" nhưng không tiêu thụ gì — đúng kiểu im lặng mà verifier phải bắt.
KHOE = "RUNNING"


def _trang_thai(ten: str) -> tuple[str, list[str], str]:
    """(trạng thái connector, danh sách trạng thái task, trace đầu tiên nếu có)."""
    code, payload = dep._req("GET", f"/connectors/{ten}/status")
    if code == 404:
        return "KHÔNG CÓ", [], ""
    if code != 200 or not isinstance(payload, dict):
        return f"HTTP {code}", [], json.dumps(payload)[:200]

    cstate = payload.get("connector", {}).get("state", "?")
    tasks = payload.get("tasks", [])
    tstates = [t.get("state", "?") for t in tasks]
    trace = next((t.get("trace", "") for t in tasks if t.get("state") != KHOE), "")
    return cstate, tstates, trace


def _dong_goc(trace: str) -> str:
    """Dòng `Caused by` SÂU NHẤT — nguyên nhân thật, không phải lớp bọc ngoài cùng.

    Kafka Connect bọc lỗi nhiều tầng: dòng đầu luôn là "Tolerance exceeded in error
    handler", chẳng nói gì. Nguyên nhân thật nằm ở `Caused by` cuối cùng.
    """
    goc = [ln.strip() for ln in trace.splitlines() if ln.strip().startswith("Caused by:")]
    return (goc[-1] if goc else trace.splitlines()[0] if trace else "")[:180]


def cmd_verify() -> int:
    mong_doi = dep.desired_connectors()

    code, payload = dep._req("GET", "/connectors")
    if code != 200 or not isinstance(payload, list):
        raise RuntimeError(f"GET /connectors ({code}): {json.dumps(payload)[:200]}")
    dang_song = set(payload)

    print(f"Đối chiếu {len(mong_doi)} connector khai trong metadata với "
          f"{dep.CONNECT_URL}:")
    print()

    loi: list[str] = []
    chu_y: list[str] = []

    for ten in sorted(mong_doi):
        cstate, tstates, trace = _trang_thai(ten)

        if cstate == "KHÔNG CÓ":
            loi.append(f"`{ten}` KHÔNG tồn tại trên Connect — chạy `connectors apply`")
            print(f"  [THIẾU] {ten}")
            continue

        if not tstates:
            # Connector RUNNING mà 0 task = không ai làm việc cả. Trông sạch, chạy rỗng.
            loi.append(f"`{ten}` có 0 task — không tiêu thụ gì dù connector `{cstate}`")
            print(f"  [LỖI  ] {ten}  connector={cstate} tasks=(rỗng)")
            continue

        xau = [s for s in tstates if s != KHOE]
        if cstate != KHOE or xau:
            # ĐÂY là lý do verifier này tồn tại: connector báo RUNNING trong khi task
            # FAILED. Ai chỉ nhìn `connector.state` sẽ thấy mọi thứ bình thường —
            # CDC đã chết hơn một ngày theo đúng cách đó.
            loi.append(f"`{ten}` connector={cstate} tasks={tstates}"
                       + (f" — {_dong_goc(trace)}" if trace else ""))
            print(f"  [LỖI  ] {ten}  connector={cstate}  tasks={tstates}")
            if trace:
                print(f"           nguyên nhân: {_dong_goc(trace)}")
            continue

        print(f"  [OK   ] {ten}  ({len(tstates)} task)")

    for ten in sorted(dang_song - set(mong_doi)):
        chu_y.append(f"`{ten}` có trên Connect nhưng metadata KHÔNG khai "
                     "(chạy `connectors apply` để dọn — xem ADR-0045)")

    print()
    for c in chu_y:
        print(f"  [chú ý] {c}")
    for e in loi:
        print(f"  [LỆCH ] {e}")

    print()
    if loi:
        print(f"KẾT QUẢ: {len(loi)} lệch, {len(chu_y)} chú ý.")
        print("Kafka Connect KHÔNG tự khởi động lại task đã chết — một sự cố thoáng qua")
        print("(Schema Registry restart, Elasticsearch dội ngược) giết task vĩnh viễn.")
        print("Khôi phục: curl -X POST "
              f"'{dep.CONNECT_URL}/connectors/<tên>/restart?includeTasks=true&onlyFailed=true'")
        return 1

    print(f"KẾT QUẢ: 0 lệch, {len(chu_y)} chú ý. "
          f"{len(mong_doi)} connector và mọi task đều {KHOE}.")
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
    except (urllib.error.URLError, RuntimeError, FileNotFoundError, OSError) as exc:
        print(f"KHÔNG đối chiếu được với Kafka Connect: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
