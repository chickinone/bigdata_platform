from __future__ import annotations

import argparse
import json
import subprocess
import sys

from ..registry import REPO_ROOT, ContractError, load_pipelines

SPARK_CONTAINER = "bigdata-spark-master"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_MASTER = "spark://spark-master:7077"
PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
ICEBERG_PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0," + PACKAGES
CONTAINER_RUNNER = "/opt/spark-jobs/medallion_runner.py"
PLAN_DIR_HOST = "spark/jobs/generated"
PLAN_DIR_CONTAINER = "/opt/spark-jobs/generated"

# Marker runner in ra khi ĐÃ ghi xong. Đây là một nguồn cho câu "thế nào là chạy xong":
# _submit (Python) và bash_command (Airflow) cùng đọc hằng này. Trước đây chỉ _submit
# đòi marker, còn BashOperator chỉ xét exit code - nên DAG báo xanh khi spark-submit
# exit 0 mà job không ghi gì. Orchestrator báo xanh sai tệ hơn không có orchestrator.
SUCCESS_MARKER = "WROTE"

# Ma thoat rieng cho "input chua co du lieu". KHONG phai loi: tren stack vua dung, S3
# sink chua flush nen Bronze trong la trang thai binh thuong. Gop no vao ma loi se lam
# `cli apply` do moi lan dung lanh — mot cong luon do la mot cong bi bo qua (ADR-0041).
NOT_READY = 4


def _stage(spec: dict) -> int:
    """Thứ tự chạy theo phụ thuộc INPUT, không theo layer: job đọc Bronze là nguồn
    (chạy trước), job đọc Silver là dẫn xuất (chạy sau — gold + iceberg đều đọc Silver).
    """
    paths = " ".join(i["path"] for i in spec["inputs"])
    if "data-lake-bronze" in paths:
        return 0
    if "data-lake-silver" in paths:
        return 1
    return 2


def _packages(spec: dict) -> str:
    return ICEBERG_PACKAGES if spec["output"].get("format") == "iceberg" else PACKAGES


def load_batch_specs() -> list[dict]:
    # Spec đi qua registry (validate schema + kiểm trùng tên) rồi mới lọc engine —
    # không tự yaml.safe_load, để chỉ có MỘT nơi biết "spec hợp lệ là gì".
    specs = [s for s in load_pipelines() if s.get("engine") == "spark_sql"]
    return sorted(specs, key=lambda s: (_stage(s), s["name"]))


def container_plan_path(spec: dict) -> str:
    """Đường dẫn job plan TRONG container (nơi spark-submit đọc)."""
    return f"{PLAN_DIR_CONTAINER}/{spec['name']}.json"


def submit_argv(spec: dict, as_of: str | None = None, full_refresh: bool = False) -> list[str]:
    """Lệnh `docker exec ... spark-submit` chạy một batch job — một nguồn sự thật cho
    'chạy job thế nào'. Deployer dùng để submit; generator Airflow dùng để dựng
    bash_command của task (cùng một lệnh -> DAG chạy y hệt tay/deployer)."""
    env = ["-e", f"JOB_PLAN={container_plan_path(spec)}"]
    if as_of:
        env += ["-e", f"AS_OF={as_of}"]
    if full_refresh:
        env += ["-e", "FULL_REFRESH=1"]
    return [
        "docker", "exec", *env, SPARK_CONTAINER,
        SPARK_SUBMIT, "--master", SPARK_MASTER,
        # ivy về /tmp: thư mục mặc định không ghi được khi container fresh.
        "--conf", "spark.jars.ivy=/tmp/.ivy2",
        "--packages", _packages(spec), CONTAINER_RUNNER,
    ]


def bash_command(spec: dict) -> str:
    """Lệnh shell cho task Airflow: submit RỒI đòi marker thành công.

    BashOperator chỉ phán theo exit code của lệnh cuối. Nên nối trần `submit_argv` là
    đánh mất đúng cái guard mà `_submit` có (`returncode == 0 AND có WROTE`) - hai
    đường chạy cùng một lệnh nhưng khác tiêu chí đậu. Ở đây `grep` là lệnh cuối, nên
    nó mới là trọng tài: không có WROTE trong log thì task đỏ, kể cả khi exit 0.

    `tee` (không phải `grep -q` trực tiếp trên pipe) để log vẫn chảy ra Airflow theo
    thời gian thực, và để `grep` khỏi đóng pipe sớm làm spark-submit dính SIGPIPE.
    """
    log = f"/tmp/medallion_{spec['name']}.$$.log"
    # {{ds}} = đầu data interval của lần chạy. Nhờ nó, clear/backfill một ngày cũ sẽ
    # tính lại đúng cửa sổ của ngày đó mà không cần cờ riêng. Viết liền không khoảng
    # trắng để sau khi Jinja thay xong, chuỗi không cần trích dẫn thêm.
    cmd = " ".join(submit_argv(spec, as_of="{{ds}}"))
    return f"{cmd} 2>&1 | tee {log}; grep -q '^{SUCCESS_MARKER} ' {log}"


def _write_plan(spec: dict) -> str:
    """Ghi job plan JSON, trả về đường dẫn TRONG container."""
    plan = {
        "name": spec["name"],
        "inputs": spec["inputs"],
        "sql": spec["sql"],
        "output": spec["output"],
    }
    if spec.get("incremental"):
        plan["incremental"] = spec["incremental"]
    rel = f"{PLAN_DIR_HOST}/{spec['name']}.json"
    path = REPO_ROOT / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8", newline="\n")
    return container_plan_path(spec)


def cmd_plan() -> int:
    specs = load_batch_specs()
    print(f"Đã sinh {len(specs)} batch job plan (thứ tự chạy):\n")
    for spec in specs:
        _write_plan(spec)
        ins = ", ".join(i["view"] for i in spec["inputs"])
        out = spec["output"]
        target = out.get("table") or out.get("path")
        fmt = out.get("format", "parquet")
        print(f"  [stage {_stage(spec)}] {spec['name']}")
        print(f"           inputs: {ins}")
        print(f"           output: {target} ({fmt}, {len(out.get('columns', []))} cột)")
        inc = spec.get("incremental")
        if inc:
            print(f"           chạy:   incremental {inc['lookback_days']}d "
                  f"(cắt: {', '.join(inc['windowed_inputs'])})")
        else:
            print("           chạy:   full refresh — đọc lại toàn bộ lịch sử")
    print("\nChạy `apply` để spark-submit theo thứ tự.")
    return 0


def _submit(spec: dict, as_of: str | None = None, full_refresh: bool = False) -> bool:
    _write_plan(spec)
    print(f"  spark-submit {spec['name']} (layer {spec.get('layer')}) ...")
    # encoding/errors tường minh: text=True dùng locale (cp1252 trên Windows), mà log
    # Spark có byte không decode được -> thread đọc chết, proc.stdout thành None và
    # dòng ghép bên dưới nổ TypeError. Cùng họ với lỗi đã vá ở _ch_exec/_pg_scalar.
    proc = subprocess.run(submit_argv(spec, as_of, full_refresh), capture_output=True,
                          text=True, encoding="utf-8", errors="replace")
    out = (proc.stdout + proc.stderr).splitlines()
    wrote = [ln for ln in out if ln.startswith(SUCCESS_MARKER)]
    if proc.returncode == 0 and wrote:
        print(f"    {wrote[0]}")
        return True
    if proc.returncode == NOT_READY:
        # Input chua co du lieu — khac han voi loi. Xem medallion_runner.
        chua = [ln for ln in out if ln.startswith("CHUA SAN SANG")]
        print(f"    {chua[0] if chua else 'CHUA SAN SANG: input chua co du lieu'}")
        return NOT_READY
    print(f"    LỖI (exit {proc.returncode}):")
    print("      " + "\n      ".join(out[-8:]))
    return False


def cmd_apply(as_of: str | None = None, full_refresh: bool = False) -> int:
    specs = load_batch_specs()
    scope = "TOÀN BỘ (full refresh)" if full_refresh else f"cửa sổ tới {as_of or 'hôm nay'}"
    print(f"Chạy {len(specs)} batch job theo thứ tự layer — {scope}:\n")
    failed = 0
    chua_san_sang = False
    for spec in specs:
        kq = _submit(spec, as_of, full_refresh)
        if kq is not True and kq == NOT_READY:
            chua_san_sang = True
            print("    -> dừng chuỗi: chưa có dữ liệu để tính (KHÔNG phải lỗi).")
            break
        if not kq:
            failed += 1
            print("    -> dừng chuỗi (job sau có thể phụ thuộc job này).")
            break
    print()
    if chua_san_sang:
        print("KẾT QUẢ: BỎ QUA — Bronze chưa có dữ liệu (S3 sink chưa flush).")
        print("Không phải lỗi. Chạy lại sau vài phút; nếu sink HỎNG thì "
              "`cli verify` (connect_health) mới là chỗ báo.")
        return 0
    if failed:
        print(f"KẾT QUẢ: có job LỖI — xem trên.")
        return 1
    print(f"KẾT QUẢ: {len(specs)} batch job chạy xong.")
    return 0


def _force_utf8() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    _force_utf8()
    parser = argparse.ArgumentParser(prog="dataplatform.deployers.spark_batch")
    parser.add_argument("command", nargs="?", default="plan", choices=["plan", "apply"])
    parser.add_argument("--as-of", metavar="YYYY-MM-DD",
                        help="Ngày mốc của cửa sổ incremental. Mặc định hôm nay. "
                             "Đặt ngày cũ = tính lại cửa sổ của ngày đó (backfill).")
    parser.add_argument("--full-refresh", action="store_true",
                        help="Bỏ qua cửa sổ, tính lại toàn bộ lịch sử. Dùng khi cần "
                             "đồng bộ lại chiều đã đổi, hoặc vá dữ liệu về muộn hơn lookback.")
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            return cmd_plan()
        return cmd_apply(args.as_of, args.full_refresh)
    except (ContractError, KeyError) as exc:
        print(f"LỖI SPEC\n{exc}", file=sys.stderr)
        return 2
    except FileNotFoundError as exc:
        print(f"KHÔNG chạy được docker: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
