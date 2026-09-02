from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

from .. import state
from ..deployers import flink_metrics as dep
from ..registry import ContractError

KHOE = "RUNNING"
# Job vừa submit chưa kịp RUNNING — không tính là hỏng.
DANG_LEN = ("CREATED", "INITIALIZING", "RESTARTING", "RECONCILING")


def _rest(path: str) -> dict:
    with urllib.request.urlopen(f"{dep.FLINK_REST}{path}", timeout=15) as r:
        return json.loads(r.read().decode("utf-8"))


def _exception_gan_nhat(jid: str) -> tuple[int, str]:
    """(số lần job đã hỏng, thông điệp gốc gần nhất).

    ĐÂY là thứ bắt được crash loop. `flink list` chỉ hiện state TỨC THỜI, nên một job
    chết-rồi-restart liên tục vẫn hiện RUNNING mỗi lần ta nhìn — `on_timer` trả None đã
    ẩn được 1.104 lần lỗi theo đúng cách đó (ADR-0048). Lịch sử exception thì không nói
    dối: nó cộng dồn.
    """
    try:
        d = _rest(f"/jobs/{jid}/exceptions?maxExceptions=5")
    except (urllib.error.URLError, OSError, ValueError):
        return 0, ""
    entries = (d.get("exceptionHistory") or {}).get("entries") or []
    if not entries:
        goc = (d.get("root-exception") or "").strip()
        return (1, goc.splitlines()[0][:160]) if goc else (0, "")
    dau = entries[0]
    msg = (dau.get("exceptionName") or dau.get("stacktrace") or "").strip()
    return len(entries), msg.splitlines()[0][:160]


def cmd_verify() -> int:
    mong_doi = state.load(dep.STATE_KEY, {}) or {}

    if not mong_doi:
        print("Chưa từng chạy `flink_metrics apply` (state trống).")
        print()
        print("KẾT QUẢ: bỏ qua — không có job nào để đối chiếu. KHÔNG phải lỗi.")
        return 0

    overview = _rest("/jobs/overview")
    song = {j["jid"]: j for j in overview.get("jobs", [])}

    print(f"Đối chiếu {len(mong_doi)} job Flink đã submit với {dep.FLINK_REST}:")
    print()

    loi: list[str] = []
    chu_y: list[str] = []

    for nhan, jid in sorted(mong_doi.items()):
        j = song.get(jid)
        if j is None:
            loi.append(f"`{nhan}` ({jid[:8]}) KHÔNG còn trên cluster — "
                       "jobmanager restart (job không HA) hoặc bị huỷ tay")
            print(f"  [MẤT  ] {nhan}  {jid[:8]}")
            continue

        tt = j.get("state", "?")
        so_loi, msg = _exception_gan_nhat(jid)

        if tt != KHOE:
            muc = chu_y if tt in DANG_LEN else loi
            muc.append(f"`{nhan}` đang ở trạng thái {tt} (mong đợi {KHOE})")
            print(f"  [{'chú ý' if tt in DANG_LEN else 'LỖI  '}] {nhan}  {jid[:8]}  {tt}")
            continue

        if so_loi:
            # RUNNING mà vẫn có lịch sử hỏng = đã chết và tự restart. Đây là crash loop.
            loi.append(f"`{nhan}` đang RUNNING nhưng đã hỏng {so_loi} lần"
                       + (f" — {msg}" if msg else ""))
            print(f"  [LỖI  ] {nhan}  {jid[:8]}  RUNNING nhưng có {so_loi} lần hỏng")
            if msg:
                print(f"           gần nhất: {msg}")
            continue

        print(f"  [OK   ] {nhan}  {jid[:8]}  RUNNING, 0 lần hỏng")

    for jid, j in sorted(song.items()):
        if j.get("state") == KHOE and jid not in set(mong_doi.values()):
            chu_y.append(f"job `{j.get('name', '?')[:40]}` ({jid[:8]}) đang RUNNING nhưng "
                         "KHÔNG có trong state — bản trùng? `flink_metrics apply` sẽ dọn")

    print()
    for c in chu_y:
        print(f"  [chú ý] {c}")
    for e in loi:
        print(f"  [LỆCH ] {e}")

    print()
    if loi:
        print(f"KẾT QUẢ: {len(loi)} lệch, {len(chu_y)} chú ý.")
        print("Job Flink KHÔNG có HA: jobmanager restart là mất job, phải "
              "`python -m dataplatform.deployers.flink_metrics apply` để submit lại.")
        return 1

    print(f"KẾT QUẢ: 0 lệch, {len(chu_y)} chú ý. "
          f"{len(mong_doi)} job đều RUNNING và chưa hỏng lần nào.")
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
    except (urllib.error.URLError, RuntimeError, OSError, ValueError) as exc:
        print(f"KHÔNG đối chiếu được với Flink: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
