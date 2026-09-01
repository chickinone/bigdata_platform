from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from . import compat
from .generators import (
    airflow_dag,
    clickhouse_ddl,
    debezium,
    dlq,
    es_sink,
    lineage,
    postgres_publication,
    s3_sink,
    topic_manifest,
    trino_catalog,
)
from .registry import (
    REPO_ROOT,
    ContractError,
    connections_by_name,
    load_connections,
    load_datasets,
    load_pipelines,
)

# Ghi JSON với indent 2 + newline cuối file. Đây là QUY ước, không phải yêu cầu
# của Kafka Connect - chọn một kiểu rồi giữ nguyên để diff giữa các lần chạy chỉ
# phản ánh thay đổi thật.
JSON_INDENT = 2


def _serialize(payload) -> str:
    """Biến artifact thành text để ghi ra đĩa.

    Hai loại artifact:
      - dict  -> JSON (connector config, bản kê DLQ...)
      - str   -> text nguyên văn (DDL SQL, publication...)
    """
    if isinstance(payload, str):
        return payload
    return json.dumps(payload, indent=JSON_INDENT, ensure_ascii=False) + "\n"


# Thu muc ma control plane so huu TRON VEN: moi file o day deu la ban sinh. Nho vay
# phat hien file THUA khong can state — bat cu gi khop glob ma khong nam trong _collect()
# deu la rac cua mot dataset da bi xoa khoi metadata/.
#
# Chi liet ke duoc nhung glob co hinh dang "N dataset -> N file". Artifact don le
# (debezium/postgres-connector.json, kafka/topics.json...) khong bao gio thanh thua vi
# chung luon duoc sinh, chi noi dung thay doi.
OWNED_GLOBS = [
    "kafka-connect/es-sinks/*.json",
    "kafka-connect/s3-sinks/*.json",
    "trino/etc/catalog/*.properties",
]


def _orphan_files(targets: dict) -> list:
    """File nam trong vung control plane so huu nhung KHONG con duoc sinh nua.

    Day la nua con thieu cua "nguon su that duy nhat": truoc day `metadata/` quyet dinh
    duoc cai gi PHAI ton tai, nhung khong quyet dinh duoc cai gi KHONG DUOC ton tai.
    Xoa `transfers.yaml` roi `write` thi es-sink-transfers.json van nam do vinh vien, va
    `check` bao khop tuyet doi vi no chi nhin nhung file MINH SINH RA.
    """
    wanted = set(targets)
    orphans = []
    for glob in OWNED_GLOBS:
        for path in sorted(REPO_ROOT.glob(glob)):
            rel = path.relative_to(REPO_ROOT).as_posix()
            if rel not in wanted:
                orphans.append(rel)
    return orphans


def _collect() -> dict:
    datasets = load_datasets()
    conns = connections_by_name()
    targets: dict = {}
    targets.update(debezium.targets(datasets, conns))
    targets.update(es_sink.targets(datasets, conns))
    targets.update(s3_sink.targets(datasets, conns))
    targets.update(dlq.targets(datasets))
    targets.update(postgres_publication.targets(datasets))
    targets.update(clickhouse_ddl.targets(datasets))
    targets.update(topic_manifest.targets(datasets, conns))
    targets.update(trino_catalog.targets(load_connections()))
    pipelines = load_pipelines()
    targets.update(lineage.targets(datasets, pipelines))
    targets.update(airflow_dag.targets(pipelines))
    return targets


# Các khoá mà giá trị là DANH sách ngăn bằng dấu phẩy, và thứ tự không mang ý
# nghĩa. Kafka Connect coi chúng như một tập hợp — so sánh như chuỗi sẽ báo lệch
# giả chỉ vì generator sắp thứ tự khác người viết tay.
SET_VALUED_KEYS = {"topics", "table.include.list"}


def _normalize(payload: dict) -> dict:
    """Đưa config về dạng so sánh được theo ngữ nghĩa.

    Đây là điểm tinh tế của `check`: "so sánh ngữ nghĩa" không chỉ là parse JSON,
    mà còn là biết khoá nào bất biến theo thứ tự.
    """
    out = {"name": payload.get("name"), "config": dict(payload.get("config", {}))}
    for key in SET_VALUED_KEYS:
        if key in out["config"]:
            items = [v.strip() for v in out["config"][key].split(",") if v.strip()]
            out["config"][key] = sorted(items)
    return out


def _compare(rel_path: str, generated) -> tuple[str, list[str]]:
    """So bản sinh với file trên đĩa. Trả về (trạng_thái, danh_sách_khác_biệt).

    Rẽ theo loại artifact:
      - dict (JSON): so ngữ nghĩa (parse rồi so dict). File viết tay có dòng trống
        + thứ tự khoá tuỳ người; ép tái tạo từng byte là giòn. Kafka Connect đọc
        JSON, không quan tâm dòng trống.
      - str (SQL/text): so nguyên văn. Lý do khác JSON: file này do control plane
        sở hữu, không công cụ ngoài nào format lại, nên byte-match là hợp lý và
        chặt hơn.
    """
    path = REPO_ROOT / rel_path
    if not path.exists():
        return "MOI", []

    # newline="" TAT universal newlines. Khong co no, Python dich CRLF -> LF NGAY LUC
    # DOC, nen phep so ben duoi khong bao gio thay khac biet ket thuc dong — check tung
    # bao "khop tuyet doi" tren file CRLF du kien lam healthcheck Trino do vinh vien
    # (ADR-0043). Voi JSON thi vo hai (json.loads bo qua khoang trang), voi text thi
    # day moi dung la "so nguyen van" nhu docstring tren kia hua.
    raw = path.read_text(encoding="utf-8", newline="")

    if isinstance(generated, str):
        if raw == generated:
            return "KHOP", []
        return "KHAC", _diff_text(raw, generated)

    current = json.loads(raw)
    if _normalize(current) == _normalize(generated):
        return "KHOP", []
    return "KHAC", _diff_keys(_normalize(current), _normalize(generated))


def _diff_text(current: str, generated: str) -> list[str]:
    """Diff dòng cho artifact text — chỉ những dòng thật sự khác."""
    import difflib

    # Khac biet CHI o ket thuc dong: splitlines() gom moi kieu xuong dong ve mot
    # moi, nen neu tach dong ra bang nhau ma chuoi tho khac nhau thi loi nam dung o
    # ky tu xuong dong. Bao tuong minh, vi diff dong se hien hai dong TRONG Y HET
    # nhau va khong ai hieu noi.
    if current.splitlines() == generated.splitlines():
        cr = sum(1 for ch in current if ch == chr(13))
        return [
            f"CHI khac KET THUC DONG — noi dung giong het ({cr} ky tu CR tren dia, ban sinh dung LF).",
            "Nguyen nhan thuong gap: core.autocrlf tren Windows doi file luc checkout.",
            "Sua: `git add --renormalize <file>` (da co quy tac trong .gitattributes). Xem ADR-0043.",
        ]

    diffs: list[str] = []
    for line in difflib.unified_diff(
        current.splitlines(), generated.splitlines(),
        fromfile="đĩa", tofile="sinh", lineterm="",
    ):
        if line.startswith(("+", "-")) and not line.startswith(("+++", "---")):
            diffs.append(line)
    return diffs[:40]  # đủ để thấy, không tràn màn hình


def _diff_keys(current: dict, generated: dict) -> list[str]:
    """Liệt kê khác biệt ở mức từng khoá config — để đọc được ngay khác chỗ nào,
    thay vì phải tự dò hai khối JSON.
    """
    diffs: list[str] = []
    if current.get("name") != generated.get("name"):
        diffs.append(f"name: {current.get('name')!r} -> {generated.get('name')!r}")

    cur_cfg = current.get("config", {})
    gen_cfg = generated.get("config", {})

    for key in sorted(set(cur_cfg) | set(gen_cfg)):
        old, new = cur_cfg.get(key), gen_cfg.get(key)
        if old == new:
            continue
        if key not in gen_cfg:
            diffs.append(f"- {key}: {old!r}  (bản sinh THIẾU)")
        elif key not in cur_cfg:
            diffs.append(f"+ {key}: {new!r}  (bản sinh THÊM)")
        else:
            diffs.append(f"~ {key}: {old!r} -> {new!r}")
    return diffs


def cmd_check() -> int:
    targets = _collect()
    drift = 0

    print(f"Đối chiếu {len(targets)} artifact sinh từ metadata/ với file trên đĩa:\n")
    for rel_path, payload in sorted(targets.items()):
        status, diffs = _compare(rel_path, payload)
        if status == "KHOP":
            print(f"  [KHỚP] {rel_path}")
        elif status == "MOI":
            print(f"  [MỚI ] {rel_path}  (chưa có trên đĩa)")
            drift += 1
        else:
            print(f"  [KHÁC] {rel_path}")
            for d in diffs:
                print(f"          {d}")
            drift += 1

    orphans = _orphan_files(targets)
    for rel_path in orphans:
        print(f"  [THỪA] {rel_path}  (không còn dataset/connection nào sinh ra file này)")

    print()
    if orphans:
        print(f"{len(orphans)} file THỪA: metadata/ không còn khai chúng nhưng file vẫn nằm trên đĩa.")
        print("Chạy `cli write` để xoá — hoặc nếu file đó cần giữ, nó không thuộc control plane.")
    if drift or orphans:
        print(f"KẾT QUẢ: {drift}/{len(targets)} artifact lệch, {len(orphans)} file thừa.")
        print("Bản sinh CHƯA tái tạo đúng hiện trạng -> chưa được cắt chuyển.")
        return 1

    print(f"KẾT QUẢ: {len(targets)}/{len(targets)} artifact khớp tuyệt đối, 0 file thừa.")
    print("Contract mang đủ thông tin để sinh lại toàn bộ file viết tay.")
    return 0


def cmd_write() -> int:
    targets = _collect()
    for rel_path, payload in sorted(targets.items()):
        path = REPO_ROOT / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        # newline="\n": ép LF kể cả trên Windows. write_text mặc định (newline=None)
        # dịch \n -> \r\n trên Windows, làm hỏng script .sh chạy trong container Linux
        # (set -euo pipefail\r -> option lỗi). Mọi artifact đều cho engine Linux nên
        # đều phải LF. `check` không thấy khác biệt này vì read_text dịch ngược lúc đọc.
        path.write_text(_serialize(payload), encoding="utf-8", newline="\n")
        print(f"  đã ghi  {rel_path}")

    # Xoá file THỪA: đây là nửa còn thiếu để `metadata/` thật sự là nguồn sự thật duy
    # nhất. Chỉ xoá trong OWNED_GLOBS — vùng control plane sở hữu trọn vẹn — và chỉ file
    # SINH được, nên xoá nhầm thì `write` lại là có ngay. Không đụng gì mang dữ liệu.
    orphans = _orphan_files(targets)
    for rel_path in orphans:
        (REPO_ROOT / rel_path).unlink()
        print(f"  đã XOÁ  {rel_path}  (thừa — metadata/ không còn khai)")

    print(f"\nĐã sinh {len(targets)} artifact từ metadata/"
          + (f", xoá {len(orphans)} file thừa." if orphans else "."))
    return 0


def cmd_show() -> int:
    for rel_path, payload in sorted(_collect().items()):
        print(f"===== {rel_path}")
        print(_serialize(payload))
    return 0


def _default_base() -> str:
    """Ref nền để so 'plan'/'compat'. CI đặt GITHUB_BASE_REF cho PR; local mặc định main."""
    ref = os.getenv("GITHUB_BASE_REF")
    return f"origin/{ref}" if ref else "origin/main"


def _compare_against(base_text: str, generated) -> tuple[str, list[str]]:
    """Như _compare nhưng so với nội DUNG ở ref nền (không phải file trên đĩa).
    Hướng diff: nền -> bản sinh từ metadata hiện tại."""
    if isinstance(generated, str):
        if base_text == generated:
            return "SAME", []
        return "ĐỔI", _diff_text(base_text, generated)
    base_obj = json.loads(base_text)
    if _normalize(base_obj) == _normalize(generated):
        return "SAME", []
    return "ĐỔI", _diff_keys(_normalize(base_obj), _normalize(generated))


def cmd_plan(base: str) -> int:
    """'terraform plan' cho metadata: artifact nào sẽ đổi khi merge PR vào `base`.

    So bản sinh từ metadata hiện tại với artifact đã commit ở `base` — reviewer thấy
    hệ quả vận hành của một thay đổi contract, không chỉ diff YAML. Thuần tĩnh + git,
    không cần engine. Informational (exit 0)."""
    targets = _collect()
    new: list[str] = []
    changed: list[tuple[str, list[str]]] = []
    for rel, payload in sorted(targets.items()):
        base_text = compat.git_show(base, rel)
        if base_text is None:
            new.append(rel)
            continue
        status, diffs = _compare_against(base_text, payload)
        if status == "ĐỔI":
            changed.append((rel, diffs))

    print(f"PLAN vs `{base}` — hệ quả khi merge:\n")
    for rel in new:
        print(f"  [MỚI ] {rel}")
    for rel, diffs in changed:
        print(f"  [ĐỔI ] {rel}")
        for d in diffs:
            print(f"          {d}")
    if not new and not changed:
        print("  (không artifact nào đổi)")
    print(f"\nKẾT QUẢ: {len(new) + len(changed)} artifact đổi — {len(new)} mới, {len(changed)} sửa.")
    return 0


def cmd_compat(base: str) -> int:
    """Gate BACKWARD: chặn thay đổi contract phá tương thích ngược (xem compat.py).
    So dataset ở `base` với working tree. Exit 1 nếu có breaking change."""
    base_ds = compat.datasets_at_ref(base)
    if not base_ds:
        print(f"Không đọc được dataset ở '{base}' (ref mới/nông?) — bỏ qua gate.")
        return 0
    cur_ds = {d.urn: d.raw for d in load_datasets()}

    breaks = 0
    print(f"COMPAT (BACKWARD) vs `{base}`:\n")
    for urn, cur in sorted(cur_ds.items()):
        base_raw = base_ds.get(urn)
        if base_raw is None:
            continue  # dataset mới — không có gì để phá
        base_cols, cur_cols = base_raw.get("columns", []), cur.get("columns", [])
        msgs = compat.compare_columns(base_cols, cur_cols)
        removed = compat.removed_columns(base_cols, cur_cols)
        if msgs:
            print(f"  [VỠ] {urn}")
            for m in msgs:
                print(f"        {m}")
            breaks += len(msgs)
        if removed:
            print(f"  [note] {urn}: xoá cột {removed} — BACKWARD cho phép, kiểm consumer.")
    for urn in sorted(set(base_ds) - set(cur_ds)):
        print(f"  [note] xoá dataset `{urn}` — kiểm consumer hạ nguồn.")

    print()
    if breaks:
        print(f"KẾT QUẢ: {breaks} thay đổi PHÁ BACKWARD -> chặn merge.")
        return 1
    print("KẾT QUẢ: không có thay đổi phá BACKWARD.")
    return 0


VERIFIERS = [
    "postgres_schema", "postgres_publication", "kafka_topics",
    "avro_schema", "clickhouse_schema", "trino_catalog", "quality",
]


def cmd_verify() -> int:
    """Chay TOAN BO verifier runtime, gop thanh mot ma thoat.

    Vi sao can lenh nay: truoc day 7 verifier la 7 diem vao roi rac, nen thuc te khong
    ai chay du. Loi ClickHouse 0 bang (ADR-0043) lot dung vi the — `clickhouse_schema`
    thua suc bat, chi la khong ai goi. Gop lai moi dat lich duoc.

    Phan biet HAI loai that bai, vi chung doi hoi hai phan ung khac han:
      - LECH  (exit 1): engine song nhung KHONG khop contract -> phai sua.
      - KHONG TOI (exit 3): engine chet/chua bat -> chua ket luan duoc gi.
    Gop chung lam mot se bien "stack chua bat" thanh bao dong gia, ma bao dong gia lap
    lai thi ca cong bi bo qua — cung ly do da chon `--no-git` cho gitleaks (ADR-0041).
    """
    import importlib

    results: list[tuple[str, int]] = []
    for name in VERIFIERS:
        print()
        print("=" * 70)
        print(name)
        print("=" * 70)
        mod = importlib.import_module(f"dataplatform.verifiers.{name}")
        try:
            rc = mod.main([])
        except SystemExit as exc:
            rc = int(exc.code or 0)
        results.append((name, rc))

    drift = [n for n, rc in results if rc == 1]
    broken = [n for n, rc in results if rc not in (0, 1)]

    print()
    print("=" * 70)
    print(f"TONG HOP {len(results)} verifier")
    print("=" * 70)
    for name, rc in results:
        mark = {0: "DAT", 1: "LECH"}.get(rc, "KHONG TOI")
        print(f"  [{mark:<9}] {name}  (exit {rc})")

    print()
    if drift:
        print(f"KET QUA: {len(drift)} verifier bao LECH -> {', '.join(drift)}")
        return 1
    if broken:
        print(f"KET QUA: khong ket luan duoc — {len(broken)} verifier khong toi duoc "
              f"engine: {', '.join(broken)}. Stack da bat chua?")
        return 3
    print(f"KET QUA: {len(results)}/{len(results)} verifier DAT — runtime khop contract.")
    return 0


# Thu tu ap len engine, suy tu phu thuoc THAT chu khong phai tu tang medallion.
# `needs` = verifier phai DAT truoc khi chay buoc nay. Day moi la phan dat gia: thu tu
# thoi chi tranh chay sai trinh tu, con `needs` chan duoc ca truong hop engine "Up"
# nhung chua san sang — dung kieu that bai da lam ClickHouse rong ma khong ai biet
# (ADR-0043): Flink submit thanh cong, roi im lang khong ghi duoc vi bang dich khong co.
APPLY_STEPS = [
    {
        "name": "connectors",
        "desc": "Debezium source + 5 ES sink + S3 sink len Kafka Connect",
        "module": "dataplatform.deployers.connectors",
        "argv": ["apply"],
        "needs": None,
    },
    {
        "name": "clickhouse",
        "desc": "DDL baseline sinh-tu-contract, roi migration co phien ban",
        "module": "dataplatform.deployers.clickhouse_migrate",
        "argv": ["baseline"],
        "then": ["apply"],
        "needs": None,
    },
    {
        "name": "flink",
        "desc": "Submit metric runner + fraud runner",
        "module": "dataplatform.deployers.flink_metrics",
        "argv": ["apply"],
        "needs": "clickhouse_schema",
    },
    {
        "name": "spark",
        "desc": "Batch medallion: Silver -> 3 Gold + Iceberg",
        "module": "dataplatform.deployers.spark_batch",
        "argv": ["apply"],
        "needs": None,
    },
]

# OpenMetadata KHONG nam trong chuoi mac dinh: no la phien rieng (phai dung stack chinh
# de nhuong RAM, va can ES cua OM song). Bat bang --with-openmetadata khi da chuan bi.
OM_STEP = {
    "name": "openmetadata",
    "desc": "Day catalog + lineage cot len OpenMetadata",
    "module": "dataplatform.deployers.openmetadata",
    "argv": ["apply"],
    "needs": None,
}


def _run_module(module: str, argv: list) -> int:
    import importlib

    mod = importlib.import_module(module)
    try:
        return mod.main(list(argv))
    except SystemExit as exc:
        return int(exc.code or 0)


def _gate(verifier: str) -> int:
    """Chay mot verifier lam DIEU KIEN TIEN QUYET. Nuot output khi DAT, chi in khi hong."""
    import contextlib
    import importlib
    import io as _io

    print(f"  [gate] {verifier} ...", end=" ")
    mod = importlib.import_module(f"dataplatform.verifiers.{verifier}")
    buf = _io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            rc = mod.main([])
    except SystemExit as exc:
        rc = int(exc.code or 0)
    print({0: "DAT", 1: "LECH"}.get(rc, "KHONG TOI"))
    if rc != 0:
        for line in buf.getvalue().splitlines()[-12:]:
            print(f"         {line}")
    return rc


def cmd_apply(as_of, full_refresh: bool, only, with_om: bool, dry_run: bool) -> int:
    """Ap TOAN BO desired state len engine, theo dung thu tu phu thuoc.

    Lop nay SOAN cac deployer chu khong THAY chung: moi buoc van goi duoc rieng le
    (`python -m dataplatform.deployers.<ten> apply`). Do la co y — luc 3 gio sang khi
    mot connector chet, ban can chay dung cai do, khong phai chay lai ca chuoi.

    Thu tu truoc day chi song trong runbook, tuc trong tri nho nguoi van hanh. Dua vao
    day de no duoc version, review va test nhu moi doan code khac.
    """
    steps = list(APPLY_STEPS) + ([OM_STEP] if with_om else [])
    if only:
        steps = [st for st in steps if st["name"] == only]
        if not steps:
            names = ", ".join(st["name"] for st in list(APPLY_STEPS) + [OM_STEP])
            print(f"Khong co buoc ten '{only}'. Co: {names}", file=sys.stderr)
            return 2

    print("=" * 70)
    print("APPLY" + (" (DRY RUN — khong dung gi)" if dry_run else "") + f" — {len(steps)} buoc")
    print("=" * 70)
    for i, st in enumerate(steps, 1):
        print(f"  {i}. {st['name']:<12} {st['desc']}")
        print(f"     {'':<12} dieu kien truoc: {st.get('needs') or '-'}")
    print()
    if dry_run:
        print("DRY RUN: khong chay gi. Bo --dry-run de ap that.")
        return 0

    done = []
    for i, st in enumerate(steps, 1):
        print("-" * 70)
        print(f"BUOC {i}/{len(steps)}: {st['name']} — {st['desc']}")
        print("-" * 70)

        if st.get("needs"):
            rc = _gate(st["needs"])
            if rc != 0:
                print()
                print(f"DUNG: dieu kien truoc cua buoc `{st['name']}` KHONG dat "
                      f"({st['needs']} tra exit {rc}).")
                print("Chay buoc truoc cho xong roi thu lai, hoac --only de bo qua co y.")
                return 1 if rc == 1 else 3

        argv = list(st["argv"])
        if st["name"] == "spark":
            if as_of:
                argv += ["--as-of", as_of]
            if full_refresh:
                argv += ["--full-refresh"]

        rc = _run_module(st["module"], argv)
        if rc == 0 and st.get("then"):
            rc = _run_module(st["module"], list(st["then"]))
        if rc != 0:
            print()
            print(f"DUNG o buoc `{st['name']}` (exit {rc}). Buoc sau phu thuoc buoc nay "
                  "nen khong chay tiep.")
            return rc
        done.append(st["name"])
        print()

    print("=" * 70)
    print(f"DA AP {len(done)} buoc: {', '.join(done)}")
    print("=" * 70)
    print()
    print("Doi chieu lai runtime voi contract:")
    print()
    return cmd_verify()


def _force_utf8_output() -> None:
    """Console Windows mặc định là cp1252, không in nổi tiếng Việt và sẽ ném
    UnicodeEncodeError. Ép UTF-8 để công cụ chạy được ở mọi terminal thay vì bắt
    người dùng tự `chcp 65001` trước mỗi lần chạy.
    """
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    _force_utf8_output()
    parser = argparse.ArgumentParser(
        prog="dataplatform.cli",
        description="Sinh artifact vận hành từ dataset contract trong metadata/.",
    )
    parser.add_argument("command", choices=["check", "write", "show", "plan", "compat", "verify", "apply"])
    parser.add_argument("--base", default=None,
                        help="Git ref nền để so 'plan'/'compat' (mặc định origin/main hoặc GITHUB_BASE_REF).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Chỉ 'apply': in chuỗi bước sẽ chạy, không đụng engine nào.")
    parser.add_argument("--only", default=None, metavar="BUOC",
                        help="Chỉ 'apply': chạy đúng một bước (connectors/clickhouse/flink/spark).")
    parser.add_argument("--with-openmetadata", action="store_true",
                        help="Chỉ 'apply': thêm bước đẩy catalog OM (phiên riêng, cần OM sống).")
    parser.add_argument("--as-of", metavar="YYYY-MM-DD",
                        help="Chỉ 'apply': chuyển tiếp cho spark_batch (cửa sổ incremental).")
    parser.add_argument("--full-refresh", action="store_true",
                        help="Chỉ 'apply': chuyển tiếp cho spark_batch (tính lại toàn bộ).")
    args = parser.parse_args(argv)

    base = args.base or _default_base()
    try:
        if args.command == "plan":
            return cmd_plan(base)
        if args.command == "compat":
            return cmd_compat(base)
        if args.command == "apply":
            return cmd_apply(args.as_of, args.full_refresh, args.only,
                             args.with_openmetadata, args.dry_run)
        return {"check": cmd_check, "write": cmd_write, "show": cmd_show,
                "verify": cmd_verify}[args.command]()
    except ContractError as exc:
        print(f"LỖI CONTRACT\n{exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
