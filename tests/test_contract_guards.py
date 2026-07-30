from __future__ import annotations

from pathlib import Path

import pytest

from dataplatform.registry import (
    ContractError,
    Dataset,
    _check_unique_topics,
    _check_unique_urns,
    load_connections,
    load_datasets,
    load_pipelines,
)


def _ds(urn: str, topic: str) -> Dataset:
    return Dataset(raw={"urn": urn, "source": {"type": "app_json", "topic": topic}},
                   path=Path(f"{urn}.yaml"))


# ---------- lưới chặn ở tầng registry ----------
def test_urn_trung_bi_chan():
    with pytest.raises(ContractError, match="URN trùng"):
        _check_unique_urns([_ds("a.b.c", "t1"), _ds("a.b.c", "t2")])


def test_topic_trung_bi_chan():
    # Hai URN khác nhau là hợp lệ với _check_unique_urns, nên đây là lưới riêng:
    # trùng topic = hai contract cùng mô tả một dòng dữ liệu (ADR-0040).
    with pytest.raises(ContractError, match="source.topic trùng"):
        _check_unique_topics([_ds("a.b.c", "same"), _ds("x.y.z", "same")])


def test_urn_va_topic_khac_nhau_thi_qua():
    _check_unique_urns([_ds("a.b.c", "t1"), _ds("x.y.z", "t2")])
    _check_unique_topics([_ds("a.b.c", "t1"), _ds("x.y.z", "t2")])


# ---------- metadata thật trong repo phải luôn hợp lệ ----------
def test_metadata_that_hop_le():
    assert len(load_datasets()) >= 1
    assert len(load_connections()) >= 1
    assert len(load_pipelines()) >= 1


def test_moi_dataset_co_chu():
    # `owner` là required trong schema; test này khoá thêm ý định: không dataset nào
    # được để owner rỗng/trắng (schema chỉ chặn minLength, không chặn khoảng trắng).
    for ds in load_datasets():
        assert ds.raw["owner"].strip(), ds.urn


def test_pipeline_tro_toi_urn_ton_tai():
    # Schema không kiểm được ràng buộc CHÉO file này: source_urn/sink_urn phải là URN
    # thật. Sai một chữ thì generator vỡ bằng KeyError lúc build job plan.
    urns = {d.urn for d in load_datasets()}
    for spec in load_pipelines():
        for key in ("source_urn", "sink_urn"):
            if key in spec:
                assert spec[key] in urns, f"{spec['name']}.{key} = {spec[key]} không tồn tại"
