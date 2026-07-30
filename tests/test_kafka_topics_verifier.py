from __future__ import annotations

from dataplatform.verifiers.kafka_topics import compare, parse_describe

# Output thật của `kafka-topics --describe` trên confluentinc/cp-kafka:7.7.1.
# Giữ nguyên tab: parser tách field theo tab, dòng chi tiết partition thì thụt lề.
SAMPLE = (
    "Topic: bankdb.public.transactions\tTopicId: aBc\tPartitionCount: 1\t"
    "ReplicationFactor: 1\tConfigs: \n"
    "\tTopic: bankdb.public.transactions\tPartition: 0\tLeader: 1\tReplicas: 1\tIsr: 1\n"
    "Topic: _connect_offsets\tTopicId: xYz\tPartitionCount: 25\tReplicationFactor: 1\t"
    "Configs: cleanup.policy=compact,segment.bytes=104857600\n"
    "\tTopic: _connect_offsets\tPartition: 0\tLeader: 1\tReplicas: 1\tIsr: 1\n"
    "Topic: __consumer_offsets\tTopicId: qQq\tPartitionCount: 50\tReplicationFactor: 1\t"
    "Configs: cleanup.policy=compact\n"
)


def _want(partitions: int = 1, rf: int = 1, configs: dict | None = None) -> dict:
    return {"partitions": partitions, "replication_factor": rf, "configs": configs or {}}


# ---------- parser ----------
def test_parse_bo_qua_dong_chi_tiet_partition():
    got = parse_describe(SAMPLE)
    assert set(got) == {"bankdb.public.transactions", "_connect_offsets", "__consumer_offsets"}


def test_parse_configs_rong_va_nhieu_config():
    got = parse_describe(SAMPLE)
    assert got["bankdb.public.transactions"]["configs"] == {}
    assert got["_connect_offsets"]["configs"] == {
        "cleanup.policy": "compact", "segment.bytes": "104857600",
    }
    assert got["_connect_offsets"]["partitions"] == 25


def test_parse_output_rong():
    assert parse_describe("") == {}


# ---------- so sánh ----------
def test_khop_hoan_toan():
    have = parse_describe(SAMPLE)
    want = {
        "bankdb.public.transactions": _want(),
        "_connect_offsets": _want(25, 1, {"cleanup.policy": "compact"}),
    }
    errors, warnings = compare(want, have)
    assert errors == []
    # __consumer_offsets có trên broker nhưng không khai -> whitelist phải chặn nhiễu,
    # nếu không verifier báo THỪA vĩnh viễn rồi bị bỏ qua.
    assert warnings == []


def test_thieu_topic_la_error():
    errors, _ = compare({"chua.co": _want()}, parse_describe(SAMPLE))
    assert len(errors) == 1 and "THIẾU" in errors[0]


def test_lech_partitions_va_rf_la_error():
    have = parse_describe(SAMPLE)
    errors, _ = compare({"_connect_offsets": _want(3, 2, {"cleanup.policy": "compact"})}, have)
    assert any("partitions lệch" in e for e in errors)
    assert any("RF lệch" in e for e in errors)


def test_topic_khong_khai_chi_la_warning():
    errors, warnings = compare({"bankdb.public.transactions": _want()}, parse_describe(SAMPLE))
    assert errors == []
    assert any("THỪA" in w and "_connect_offsets" in w for w in warnings)


def test_chi_so_khoa_ma_ban_ke_khai():
    # Broker tự đặt segment.bytes; bản kê cố ý không khai giá trị mặc định (ADR-0020).
    # So toàn bộ config sẽ báo lệch giả và làm verifier mất tin cậy.
    have = parse_describe(SAMPLE)
    errors, _ = compare({"_connect_offsets": _want(25, 1, {"cleanup.policy": "compact"})}, have)
    assert errors == []
