import sys
from datetime import date
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "spark" / "jobs"))

# Import được vì medallion_runner để pyspark import bên trong hàm — phần thuần Python
# dưới đây là thứ quyết định partition nào bị ghi đè, nên nó phải chạy trong CI tĩnh.
import medallion_runner as mr  # noqa: E402

from dataplatform.deployers.spark_batch import (  # noqa: E402
    SUCCESS_MARKER, bash_command, load_batch_specs, submit_argv,
)


def _inc(**over):
    base = {"lookback_days": 3, "date_columns": ["year", "month", "day"],
            "windowed_inputs": ["silver"]}
    base.update(over)
    return base


def _plan(**over):
    plan = {
        "name": "t",
        "incremental": _inc(),
        "output": {"format": "parquet", "mode": "overwrite",
                   "partition_by": ["year", "month", "day"]},
    }
    for k, v in over.items():
        if k == "output":
            plan["output"].update(v)
        elif k == "incremental":
            plan["incremental"].update(v)
    return plan


# --- toán cửa sổ ---

def test_lookback_gom_ca_as_of():
    # 3 ngày với as_of 14/08 là 12,13,14 — KHÔNG phải 11..14. Lệch một ngày ở đây
    # nghĩa là mỗi lần chạy tính thừa/thiếu trọn một partition.
    start, end, _ = mr.window_bounds(_inc(), date(2026, 8, 14))
    assert (start, end) == (date(2026, 8, 12), date(2026, 8, 14))


def test_lookback_mot_ngay_la_dung_ngay_do():
    start, end, _ = mr.window_bounds(_inc(lookback_days=1), date(2026, 8, 14))
    assert start == end == date(2026, 8, 14)


def test_margin_chi_lui_moc_doc_khong_doi_cua_so():
    start, end, read_from = mr.window_bounds(_inc(input_margin_days=1), date(2026, 8, 14))
    assert read_from == date(2026, 8, 11)
    # Bất biến: margin nới phía ĐỌC, không nới phía GHI. Nếu nó kéo cửa sổ ghi rộng
    # ra thì partition ngoài cửa sổ sẽ bị ghi đè bằng dữ liệu tính dở.
    assert (start, end) == (date(2026, 8, 12), date(2026, 8, 14))


def test_margin_mac_dinh_bang_khong():
    start, _, read_from = mr.window_bounds(_inc(), date(2026, 8, 14))
    assert read_from == start


def test_cua_so_bat_qua_giao_thang():
    start, end, _ = mr.window_bounds(_inc(lookback_days=5), date(2026, 3, 2))
    assert (start, end) == (date(2026, 2, 26), date(2026, 3, 2))


def test_cua_so_bat_qua_nam_nhuan():
    start, _, _ = mr.window_bounds(_inc(lookback_days=2), date(2024, 3, 1))
    assert start == date(2024, 2, 29)


# --- biểu thức lọc ---

def test_predicate_so_theo_ngay_khong_so_tung_cot():
    # So rời từng cột (year >= 2026 AND month >= 12 AND day >= 30) sẽ loại nhầm
    # tháng 1 năm sau. make_date ghép lại rồi mới so nên qua được chỗ giao năm.
    expr = mr.date_predicate(["year", "month", "day"], date(2026, 12, 30))
    assert expr == "make_date(year, month, day) >= date'2026-12-30'"


def test_predicate_co_can_tren_khi_ghi():
    expr = mr.date_predicate(["year", "month", "day"], date(2026, 8, 12), date(2026, 8, 14))
    assert expr == ("make_date(year, month, day) >= date'2026-08-12'"
                    " AND make_date(year, month, day) <= date'2026-08-14'")


# --- chốt chặn thảm hoạ ---

def test_plan_hop_le_khong_bao_loi():
    assert mr.incremental_problems(_plan()) == []


def test_thieu_partition_by_bi_chan():
    # Đây LÀ kịch bản thảm hoạ: không partition thì ghi đè động không có gì khoanh
    # vùng, overwrite xoá sạch path rồi ghi mỗi cửa sổ vừa tính.
    problems = mr.incremental_problems(_plan(output={"partition_by": []}))
    assert any("partition_by" in p for p in problems)


def test_date_columns_khong_phai_tien_to_bi_chan():
    problems = mr.incremental_problems(
        _plan(output={"partition_by": ["country_code", "year", "month", "day"]}))
    assert any("tiền tố" in p for p in problems)


def test_mode_append_bi_chan():
    # append + retry của Airflow = nhân bản dữ liệu. Chỉ overwrite (dynamic) mới idempotent.
    problems = mr.incremental_problems(_plan(output={"mode": "append"}))
    assert any("overwrite" in p for p in problems)


def test_iceberg_khong_dung_duong_incremental():
    problems = mr.incremental_problems(_plan(output={"format": "iceberg"}))
    assert any("parquet" in p for p in problems)


# --- as_of ---

def test_as_of_doc_duoc_chuoi_ds_cua_airflow():
    assert mr.as_of_date("2026-08-14") == date(2026, 8, 14)


def test_as_of_rong_thi_lay_hom_nay():
    assert mr.as_of_date("") == date.today()


# --- ràng buộc chéo: spec thật phải thoả bất biến ---

def test_moi_spec_incremental_that_deu_hop_le():
    for spec in load_batch_specs():
        if not spec.get("incremental"):
            continue
        plan = {"name": spec["name"], "incremental": spec["incremental"],
                "output": spec["output"]}
        assert mr.incremental_problems(plan) == [], spec["name"]


def test_windowed_inputs_phai_tro_toi_view_co_that():
    # Gõ sai tên view thì job im lặng đọc TOÀN BỘ input đó — mất hết ý nghĩa
    # incremental mà không có lỗi nào. Schema không bắt được ràng buộc chéo này.
    for spec in load_batch_specs():
        inc = spec.get("incremental")
        if not inc:
            continue
        views = {i["view"] for i in spec["inputs"]}
        assert set(inc["windowed_inputs"]) <= views, spec["name"]


def test_date_columns_phai_la_cot_co_trong_contract_output():
    for spec in load_batch_specs():
        inc = spec.get("incremental")
        if not inc:
            continue
        cols = {c["name"] for c in spec["output"].get("columns", [])}
        assert set(inc["date_columns"]) <= cols, spec["name"]


# --- tiêu chí thành công dùng chung ---

def test_bash_command_lay_grep_lam_trong_tai():
    spec = next(s for s in load_batch_specs() if s["name"] == "silver_enriched_transactions")
    cmd = bash_command(spec)
    # Lệnh CUỐI quyết định exit code của BashOperator. Nếu grep không đứng cuối thì
    # DAG lại quay về chỉ xét exit code của spark-submit.
    assert cmd.rstrip().split(";")[-1].strip().startswith(f"grep -q '^{SUCCESS_MARKER} '")


def test_bash_command_truyen_ds_cua_airflow():
    spec = next(s for s in load_batch_specs() if s["name"] == "silver_enriched_transactions")
    assert "AS_OF={{ds}}" in bash_command(spec)


def test_bash_command_khong_lam_hong_chuoi_python_sinh_ra():
    # Generator nhúng chuỗi này bằng repr; kiểm chứng nó quay vòng được.
    spec = next(s for s in load_batch_specs() if s["name"] == "silver_enriched_transactions")
    cmd = bash_command(spec)
    assert eval(repr(cmd)) == cmd  # noqa: S307


def test_deployer_khong_truyen_ds_chua_thay_the():
    # submit_argv của deployer chạy qua subprocess (không có shell, không có Jinja),
    # nên chuỗi {{ds}} lọt vào đây sẽ tới thẳng docker exec dưới dạng chữ.
    spec = next(s for s in load_batch_specs() if s["name"] == "silver_enriched_transactions")
    assert not any("{{" in a for a in submit_argv(spec, as_of="2026-08-14"))


def test_full_refresh_bat_co_moi_truong():
    spec = next(s for s in load_batch_specs() if s["name"] == "silver_enriched_transactions")
    assert "FULL_REFRESH=1" in submit_argv(spec, full_refresh=True)
    assert "FULL_REFRESH=1" not in submit_argv(spec)


@pytest.mark.parametrize("name", ["gold_customer_lifetime_metrics", "iceberg_silver_enriched"])
def test_hai_job_khong_partition_aligned_van_full_refresh(name):
    # Không phải bỏ sót: gộp trọn đời và bản sao Iceberg đều không khoanh được theo
    # ngày. Test này giữ cho lần sau không ai bật incremental cho chúng mà quên vì sao.
    spec = next(s for s in load_batch_specs() if s["name"] == name)
    assert "incremental" not in spec
