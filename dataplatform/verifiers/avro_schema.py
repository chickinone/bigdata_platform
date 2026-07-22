"""Đối chiếu contract CDC với schema Avro thật trong Schema Registry.

    python -m dataplatform.verifiers.avro_schema

Đây là nguồn sự thật độc lập thứ ba, và nó thấy thứ hai verifier kia không thấy:
**cách dữ liệu được mã hoá trên dây** (trên Kafka), không phải trong DB nguồn hay
bảng đích.

Ví dụ quyết định: cột `balance` là `numeric(19,4)` trong Postgres, nhưng trên dây
nó là **`string`** — vì `decimal.handling.mode=string` (ADR-0003). Contract phải
khai `encoded_as: string` cho đúng, và generator dựa vào đó để chèn `CAST`. Nếu
contract quên `encoded_as`, Flink sẽ hiểu sai kiểu → hỏng. Chỉ đối chiếu với Avro
thật mới bắt được sai lệch này; Postgres verifier chỉ thấy `numeric`, không thấy
`string` trên dây.

Cấu trúc message Debezium: envelope `{before, after, op, ts_ms, ...}`. `before` và
`after` cùng dùng một record (tên `Value`); `after` thường chỉ THAM chiếu tên record
đã định nghĩa đầy đủ ở `before`. Verifier lấy field từ record đó.

Chỉ áp cho dataset CDC (Avro). Dataset app_json (metric/alert) là JSON trần, không
có schema trong registry. Dataset có bảng rỗng chưa produce message nào -> chưa có
subject -> verifier bỏ QUA (không phải lỗi).
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

from ..generators.debezium import cdc_datasets
from ..registry import ContractError, Dataset, load_datasets

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL_HOST", "http://localhost:8081")

# Kiểu logic -> tập kiểu Avro chấp nhận (khi không encoded_as: string).
# timestamp nhận cả string (ZonedTimestamp, cho timestamptz) lẫn long (Timestamp,
# cho timestamp thường) — tuỳ kiểu cột Postgres, nên chấp nhận cả hai.
_AVRO_OK = {
    "long": {"long"},
    "int": {"int"},
    "string": {"string"},
    "boolean": {"boolean"},
    "timestamp": {"string", "long"},
    "date": {"int", "string"},
    "double": {"double"},
}


def _sr_get(path: str):
    url = f"{SCHEMA_REGISTRY_URL}{path}"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, None


def _base_type(t) -> str:
    """Tên kiểu Avro cơ sở của một field type (đã bỏ null). Với type có logicalType/
    connect.name (vd ZonedTimestamp) thì cơ sở vẫn là 'string'/'long' — đúng cái ta
    cần để kiểm mã hoá.
    """
    if isinstance(t, dict):
        return t.get("type")
    return t  # chuỗi thô: 'long','string',...


def _wire_columns(subject: str) -> dict[str, tuple[str, bool]] | None:
    """{tên cột -> (kiểu Avro cơ sở, nullable)} lấy từ record `before`/`after`.
    None nếu subject chưa tồn tại (bảng rỗng / chưa produce).
    """
    code, payload = _sr_get(f"/subjects/{subject}/versions/latest")
    if code == 404 or payload is None:
        return None
    schema = json.loads(payload["schema"])

    # Tìm record định nghĩa đầy đủ: quét cả before lẫn after, lấy cái là dict-record
    # có 'fields' (after có thể chỉ là tham chiếu tên).
    record = None
    for fname in ("before", "after"):
        fields = [f for f in schema["fields"] if f["name"] == fname]
        if not fields:
            continue
        ftype = fields[0]["type"]
        members = ftype if isinstance(ftype, list) else [ftype]
        for m in members:
            if isinstance(m, dict) and m.get("type") == "record" and "fields" in m:
                record = m
                break
        if record:
            break
    if record is None:
        raise RuntimeError(f"subject {subject}: không tìm thấy record before/after có fields")

    cols: dict[str, tuple[str, bool]] = {}
    for f in record["fields"]:
        t = f["type"]
        nullable = False
        if isinstance(t, list):
            nullable = "null" in t
            non_null = [x for x in t if x != "null"]
            t = non_null[0] if non_null else "null"
        cols[f["name"]] = (_base_type(t), nullable)
    return cols


def verify_dataset(ds: Dataset):
    """Trả (errors, warnings) hoặc None nếu bỏ QUA (chưa có schema)."""
    subject = f"{ds.topic}-value"
    wire = _wire_columns(subject)
    if wire is None:
        return None

    errors: list[str] = []
    warnings: list[str] = []
    contract = {c["name"]: c for c in ds.columns()}

    for name, c in contract.items():
        if name not in wire:
            errors.append(f"cột '{name}' trong contract nhưng KHÔNG có trên dây")
            continue
        avro_type, avro_null = wire[name]
        logical = c["type"]

        if c.get("encoded_as") == "string":
            # Khẳng định quan trọng nhất: contract khai string -> dây phải là string.
            if avro_type != "string":
                errors.append(f"cột '{name}': contract encoded_as=string nhưng dây là '{avro_type}'")
        elif logical.startswith("decimal("):
            if avro_type == "string":
                # Dây là string mà contract quên encoded_as -> generator sẽ thiếu CAST.
                warnings.append(f"cột '{name}': dây là string nhưng contract THIẾU encoded_as=string")
            elif avro_type != "bytes":
                errors.append(f"cột '{name}' decimal: dây là '{avro_type}' (kỳ vọng bytes/string)")
        else:
            if avro_type not in _AVRO_OK.get(logical, set()):
                errors.append(f"cột '{name}' kiểu dây lệch: contract={logical} vs Avro='{avro_type}'")

        contract_nullable = c.get("nullable", True)
        if contract_nullable != avro_null:
            warnings.append(
                f"cột '{name}' nullable lệch: contract={contract_nullable} vs Avro={'YES' if avro_null else 'NO'}"
            )

    for name in wire:
        if name not in contract:
            warnings.append(f"cột '{name}' trên dây nhưng contract THIẾU")

    return errors, warnings


def cmd_verify() -> int:
    datasets = cdc_datasets(load_datasets())
    print(f"Đối chiếu {len(datasets)} contract CDC với Avro thật "
          f"(Schema Registry: {SCHEMA_REGISTRY_URL}):\n")

    total_err = total_warn = skipped = 0
    for ds in datasets:
        result = verify_dataset(ds)
        if result is None:
            skipped += 1
            print(f"  [BỎ QUA] {ds.urn}  (chưa có schema — bảng rỗng/chưa produce)")
            continue
        errors, warnings = result
        total_err += len(errors)
        total_warn += len(warnings)
        if not errors and not warnings:
            print(f"  [KHỚP] {ds.urn}")
            continue
        print(f"  [{'LỆCH' if errors else 'CHÚ Ý'}] {ds.urn}")
        for e in errors:
            print(f"          ✗ {e}")
        for w in warnings:
            print(f"          ~ {w}")

    print()
    print(f"KẾT QUẢ: {total_err} lệch, {total_warn} chú ý, {skipped} bỏ qua.")
    if total_err:
        print("Contract KHÔNG khớp mã hoá thật trên dây — sửa contract (thường là encoded_as).")
        return 1
    if skipped and total_err == 0:
        print("Phần kiểm được đều khớp; còn dataset chưa có schema (cần produce data để kiểm nốt).")
        return 0
    print("Mọi contract CDC khớp Avro thật trên dây.")
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
    except urllib.error.URLError as exc:
        print(f"KHÔNG nối được Schema Registry ở {SCHEMA_REGISTRY_URL}: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
