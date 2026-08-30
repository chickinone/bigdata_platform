import ast
from pathlib import Path

import pytest

# Image Spark chạy Python 3.8, còn CI và máy dev chạy 3.12+. Khe hở đó từng để lọt
# `str | None` vào medallion_runner: test tĩnh xanh, job chết ngay khi submit thật.
# Hai kiểm tra dưới đây đóng khe đó mà không cần thêm phụ thuộc nào.
SPARK_PY = (3, 8)
JOBS_DIR = Path(__file__).resolve().parents[1] / "spark" / "jobs"
JOB_FILES = sorted(p for p in JOBS_DIR.glob("*.py"))


def test_co_file_de_kiem():
    assert JOB_FILES, f"không thấy file .py nào trong {JOBS_DIR}"


@pytest.mark.parametrize("path", JOB_FILES, ids=lambda p: p.name)
def test_cu_pháp_hop_le_voi_python_cua_container(path):
    # feature_version chặn cú pháp mới hơn 3.8 (vd match statement).
    ast.parse(path.read_text(encoding="utf-8"), filename=str(path),
              feature_version=SPARK_PY)


@pytest.mark.parametrize("path", JOB_FILES, ids=lambda p: p.name)
def test_file_co_annotation_phai_co_future_import(path):
    """`str | None` trong annotation là cú pháp HỢP LỆ ở mọi phiên bản — nó chỉ nổ lúc
    Python dựng hàm. Nên feature_version ở trên không bắt được; thứ bắt được là
    `from __future__ import annotations`, biến mọi annotation thành chuỗi lười."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

    annotated = any(
        node.returns is not None or any(a.annotation for a in node.args.args)
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ) or any(isinstance(node, ast.AnnAssign) for node in ast.walk(tree))
    if not annotated:
        pytest.skip("file không dùng annotation")

    has_future = any(
        isinstance(node, ast.ImportFrom) and node.module == "__future__"
        and any(alias.name == "annotations" for alias in node.names)
        for node in ast.walk(tree)
    )
    assert has_future, (
        f"{path.name} có annotation nhưng thiếu `from __future__ import annotations`; "
        f"cú pháp kiểu `str | None` sẽ chết trên Python {'.'.join(map(str, SPARK_PY))} "
        "của container Spark"
    )
