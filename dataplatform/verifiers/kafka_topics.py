"""Đối chiếu bản kê topic với Kafka thật (kafka-topics --describe).

    python -m dataplatform.verifiers.kafka_topics

Đóng lỗ hổng runtime của chặng topic, y hệt hình dạng lỗ hổng publication (ADR-0039):
`cli check` chỉ chứng minh `kafka/topics.json` khớp metadata — nó không biết broker
đang có topic nào. Mà `create-topics.sh` chỉ chạy khi dựng stack (service `kafka-init`,
`restart: "no"`), nên thêm dataset trên cluster đang sống thì manifest đúng, `check`
xanh, mà topic thật chưa tồn tại.

Với `auto.create.topics.enable=false` (ADR-0020), topic thiếu KHÔNG âm thầm như
publication — connector/Flink sẽ lỗi khi ghi. Nhưng lỗi nổ ở tầng consumer, cách xa
nguyên nhân: log Connect báo "topic không tồn tại" chứ không báo "anh quên chạy
create-topics.sh". Verifier này rút ngắn khoảng cách đó.

Ba loại lệch, mức nghiêm trọng khác nhau:
  - THIẾU  (manifest có, broker không)  -> error. Pipeline sẽ đứt ở topic này.
  - LỆCH   (partitions / RF khác)       -> error. Partition ảnh hưởng mức song song và
    phân bố key_by; RF ảnh hưởng độ bền. Cả hai KHÔNG tự sửa được bằng --if-not-exists.
  - THỪA   (broker có, manifest không)  -> warning. Thường là topic rác từ thời
    auto-create còn bật, hoặc dataset đã xoá mà chưa dọn broker.

Config: chỉ so những KHOÁ mà manifest khai (giống cách deployer connector chỉ xét khoá
trong desired). Manifest cố ý không khai giá trị mặc định broker (ADR-0020), nên so
toàn bộ sẽ báo lệch giả với mọi override mà broker tự đặt.

Chỉ ĐỌC. Sửa là chạy lại `create-topics.sh` (cho THIẾU) hoặc quyết định có chủ ý
(cho LỆCH/THỪA — đổi partition không thể rollback dễ).
"""
from __future__ import annotations

import os
import subprocess
import sys

from ..generators.topic_manifest import _entries
from ..registry import ContractError, connections_by_name, endpoint, load_datasets

KAFKA_CONTAINER = os.getenv("KAFKA_CONTAINER", "bigdata-kafka")

# Topic do chính broker quản, tồn tại bất kể auto.create.topics — control plane không
# khai và cũng không xoá được. Không whitelist thì nó luôn báo THỪA (nhiễu vĩnh viễn).
_BROKER_MANAGED = {"__consumer_offsets", "__transaction_state"}


def _describe(bootstrap: str) -> dict[str, dict]:
    """{tên topic -> {partitions, replication_factor, configs}} từ broker."""
    proc = subprocess.run(
        ["docker", "exec", "-i", KAFKA_CONTAINER,
         "kafka-topics", "--bootstrap-server", bootstrap, "--describe"],
        capture_output=True, text=True, encoding="utf-8",
    )
    if proc.returncode != 0:
        raise RuntimeError(f"kafka-topics lỗi (container {KAFKA_CONTAINER}):\n{proc.stderr.strip()}")
    return parse_describe(proc.stdout)


def parse_describe(stdout: str) -> dict[str, dict]:
    """Parse output `kafka-topics --describe` thành dict.

    Dòng TỔNG kết của mỗi topic không thụt lề và có `PartitionCount:`; các dòng chi
    tiết từng partition thì thụt lề (bỏ qua). Tách riêng khỏi subprocess để test được
    parser mà không cần broker sống.
    """
    out: dict[str, dict] = {}
    for line in stdout.splitlines():
        if not line or line[0].isspace() or "PartitionCount:" not in line:
            continue
        fields: dict[str, str] = {}
        for part in line.split("\t"):
            part = part.strip()
            if ":" in part:
                k, v = part.split(":", 1)
                fields[k.strip()] = v.strip()
        name = fields.get("Topic")
        if not name:
            continue
        configs = {}
        for kv in fields.get("Configs", "").split(","):
            kv = kv.strip()
            if "=" in kv:
                k, v = kv.split("=", 1)
                configs[k.strip()] = v.strip()
        out[name] = {
            "partitions": int(fields.get("PartitionCount", "0")),
            "replication_factor": int(fields.get("ReplicationFactor", "0")),
            "configs": configs,
        }
    return out


def compare(want: dict[str, dict], have: dict[str, dict]) -> tuple[list[str], list[str]]:
    """Trả (errors, warnings). `want` từ manifest, `have` từ broker."""
    errors: list[str] = []
    warnings: list[str] = []

    for name in sorted(set(want) - set(have)):
        errors.append(f"THIẾU `{name}` trên broker — chạy lại create-topics.sh")

    for name in sorted(set(have) - set(want) - _BROKER_MANAGED):
        warnings.append(
            f"THỪA `{name}` trên broker — không dataset/DLQ/hạ tầng nào khai "
            "(topic rác từ thời auto-create, hoặc dataset đã xoá)"
        )

    for name in sorted(set(want) & set(have)):
        w, h = want[name], have[name]
        if w["partitions"] != h["partitions"]:
            errors.append(
                f"`{name}` partitions lệch: manifest={w['partitions']} vs broker={h['partitions']}"
                " — --if-not-exists KHÔNG sửa được topic đã tồn tại"
            )
        if w["replication_factor"] != h["replication_factor"]:
            errors.append(
                f"`{name}` RF lệch: manifest={w['replication_factor']} "
                f"vs broker={h['replication_factor']}"
            )
        # Chỉ so khoá manifest khai — xem docstring module.
        for key, val in sorted(w["configs"].items()):
            actual = h["configs"].get(key)
            if actual != val:
                errors.append(f"`{name}` config `{key}` lệch: manifest={val!r} vs broker={actual!r}")

    return errors, warnings


def cmd_verify() -> int:
    conns = connections_by_name()
    bootstrap = endpoint(conns, "kafka", "bootstrap")
    rf = int(endpoint(conns, "kafka", "replication_factor"))
    want = {t["name"]: t for t in _entries(load_datasets(), rf)}

    print(f"Đối chiếu {len(want)} topic trong bản kê với broker thật "
          f"(container: {KAFKA_CONTAINER}):\n")

    have = _describe(bootstrap)
    for name in sorted(want):
        print(f"  [{'KHỚP ' if name in have else 'THIẾU'}] {name}")

    errors, warnings = compare(want, have)
    if errors or warnings:
        print()
    for e in errors:
        print(f"  ✗ {e}")
    for w in warnings:
        print(f"  ~ {w}")

    print()
    print(f"KẾT QUẢ: {len(errors)} lệch (error), {len(warnings)} chú ý (warning).")
    if errors:
        print("Broker KHÔNG khớp bản kê — xem gợi ý ở trên.")
        return 1
    if warnings:
        print("Không có lệch phá vỡ; có cảnh báo nên xem qua.")
        return 0
    print(f"Broker khớp tuyệt đối bản kê ({len(want)} topic).")
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
    except (RuntimeError, FileNotFoundError) as exc:
        print(f"KHÔNG đối chiếu được với Kafka: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
