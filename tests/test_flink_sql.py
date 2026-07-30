from __future__ import annotations

from pathlib import Path

import pytest

from dataplatform.generators.flink_sql import (
    _assert_columns_match,
    _referenced_columns,
    _sink_type,
    _source_type,
)
from dataplatform.registry import ContractError, Dataset


def _sink(*names: str) -> Dataset:
    return Dataset(raw={"urn": "bank.metric.x",
                        "source": {"type": "app_json", "topic": "metrics.x"},
                        "columns": [{"name": n, "type": "long"} for n in names]},
                   path=Path("x.yaml"))


# ---------- ROW tối thiểu: chỉ cột thật sự được tham chiếu ----------
def test_chi_lay_cot_duoc_tham_chieu():
    pipelines = [{
        "name": "p", "source_urn": "u",
        "filter": "op = 'c'",
        "dimensions": [{"as": "t", "expr": "`after`.transaction_type"}],
        "aggregations": [{"as": "v", "expr": "SUM(CAST(`after`.amount AS DECIMAL(19, 4)))"}],
    }]
    # `status`/`currency` không xuất hiện trong expr nào -> không vào ROW (loại cột chết).
    assert _referenced_columns(pipelines, "u") == ["amount", "transaction_type"]


def test_bo_qua_pipeline_khac_source():
    pipelines = [{"name": "p", "source_urn": "khac", "aggregations": [{"as": "a", "expr": "`after`.x"}]}]
    assert _referenced_columns(pipelines, "u") == []


def test_gop_cot_cua_nhieu_pipeline_chung_source():
    pipelines = [
        {"name": "p1", "source_urn": "u", "aggregations": [{"as": "a", "expr": "`after`.amount"}]},
        {"name": "p2", "source_urn": "u", "aggregations": [{"as": "b", "expr": "`after`.status"}]},
    ]
    assert _referenced_columns(pipelines, "u") == ["amount", "status"]


# ---------- kiểu: source theo mã hoá trên dây, sink theo kiểu logic ----------
def test_source_theo_ma_hoa_sink_theo_logic():
    col = {"name": "amount", "type": "decimal(19,4)", "encoded_as": "string"}
    assert _source_type(col) == "STRING"          # trên Kafka là string (ADR-0003)
    assert _sink_type(col) == "DECIMAL(19, 4)"    # ghi ra thì là decimal


def test_timestamp_khac_nhau_hai_dau():
    col = {"name": "created_at", "type": "timestamp"}
    assert _source_type(col) == "STRING"          # ZonedTimestamp = string trên dây
    assert _sink_type(col) == "TIMESTAMP(3)"


def test_kieu_la_thi_bao_loi_ro():
    with pytest.raises(ContractError, match="chưa hỗ trợ"):
        _source_type({"name": "x", "type": "geometry"})


# ---------- chốt an toàn: spec và contract sink không thể lệch ----------
def test_khop_cot_thi_qua():
    spec = {"name": "p", "dimensions": [{"as": "tx_type", "expr": "e"}],
            "aggregations": [{"as": "tx_count", "expr": "COUNT(*)"}]}
    _assert_columns_match(spec, _sink("window_start", "window_end", "tx_type", "tx_count"))


def test_lech_ten_thi_dung_generation():
    spec = {"name": "p", "aggregations": [{"as": "tx_count", "expr": "COUNT(*)"}]}
    with pytest.raises(ContractError, match="lệch nhau"):
        _assert_columns_match(spec, _sink("window_start", "window_end", "so_luong"))


def test_dung_cot_nhung_SAI_THU_TU_van_bi_chan():
    # Thứ tự quan trọng thật: ClickHouse bảng đích/MV khớp theo VỊ TRÍ, và Flink
    # INSERT INTO cũng vậy. Sai thứ tự = số liệu vào nhầm cột, không có lỗi nào.
    spec = {"name": "p", "aggregations": [{"as": "b", "expr": "1"}, {"as": "a", "expr": "2"}]}
    with pytest.raises(ContractError):
        _assert_columns_match(spec, _sink("window_start", "window_end", "a", "b"))


def test_rank_chen_dung_vi_tri():
    # Thứ tự sinh ra: [window_start, window_end] + rank + dimensions + aggregations.
    spec = {"name": "p",
            "rank": {"as": "rank_num", "partition_by": ["window_start"], "order_by": "v DESC", "keep": 10},
            "dimensions": [{"as": "account_id", "expr": "e"}],
            "aggregations": [{"as": "v", "expr": "SUM(1)"}]}
    _assert_columns_match(spec, _sink("window_start", "window_end", "rank_num", "account_id", "v"))
