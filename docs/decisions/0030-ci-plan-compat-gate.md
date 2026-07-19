# ADR-0030: CI plan → compat gate — GitOps cho contract (mở Pha 7)

- **Status:** Accepted — `plan`/`compat` chạy + verify (chặn breaking, cho qua additive), cắm vào CI
- **Date:** 2026-07-20
- **Deciders:** Phan Trường

## Bối cảnh

Pha 2–6 làm metadata thành nguồn sự thật: artifact sinh từ contract, `check` chặn drift. Nhưng còn hai
lỗ ở khâu **thay đổi**:

1. Reviewer nhìn PR chỉ thấy diff YAML — **không thấy hệ quả vận hành** (đổi cột này thì connector/DDL/
   lineage nào đổi?). Phải tự suy, dễ sót.
2. Không gì chặn **breaking change**: đổi `int->string`, thêm cột required, biến optional thành required —
   những thứ Schema Registry sẽ từ chối ở runtime (BACKWARD, Pha 2), nhưng lúc đó đã muộn (producer nổ,
   consumer chết). Cần chặn ở **PR**, không phải runtime.

## Quyết định — hai lệnh CLI mới, cắm vào CI

**`cli plan [--base ref]`** — "terraform plan" cho metadata. So bản sinh từ metadata HIỆN TẠI với artifact
đã commit ở `base`, render artifact NÀO sẽ đổi khi merge (mới/sửa + diff mức khoá). Reviewer thấy hệ quả
thật, không chỉ YAML. Informational (exit 0).

**`cli compat [--base ref]`** — gate BACKWARD. Dịch luật Avro BACKWARD sang contract cột (`compat.py`), so
dataset `base` vs working tree:

| Thay đổi | Phán |
|---|---|
| Thêm cột `nullable:false` (không default) | **VỠ** — reader mới đọc data cũ thiếu cột |
| Đổi type không promote được (`long->int`, `int->string`) | **VỠ** |
| Đổi `nullable: true -> false` (optional -> required) | **VỠ** |
| Thêm cột `nullable:true`; `false->true`; promote (`int->long`, `string->bytes`) | OK |
| Xoá cột / xoá dataset | OK (Avro cho phép) — chỉ **ghi chú** để kiểm consumer |

Type "hiệu dụng" theo lớp Avro trên dây: cột `encoded_as:string` (decimal->string, ADR-0003) coi là
`string`. Exit 1 nếu có VỠ → PR ĐỎ.

CI (`metadata-check.yml`): `check` (mọi push/PR) → `compat` (chặn, chỉ PR) → `plan` (vào job summary, chỉ
PR). Cần `fetch-depth: 0` + nạp base ref. Vẫn **thuần tĩnh** (metadata + git), không engine — nhanh, rẻ.

### Vì sao gate ở CONTRACT, không ở file Avro (.avsc)

Dự án không commit `.avsc` — Avro schema do Debezium suy từ Postgres lúc chạy. Nguồn sự thật của schema là
**contract**. Nên gate đọc thẳng contract và encode luật BACKWARD ở đó, thay vì đẻ ra một generator `.avsc`
+ thư viện Avro chỉ để so. Nhẹ hơn, đúng nguồn, không thêm dependency (chỉ PyYAML + git đã có). Luật khớp
đúng thứ Schema Registry chặn trên dây.

## Kiểm chứng (đo thật, local, `--base HEAD`)

- **Breaking bị chặn:** đổi `risk_score int->string` + thêm `segment nullable:false` → compat in đúng 2 dòng
  VỠ, **exit 1**. `plan` chỉ ra `lineage/graph.json` đổi (thêm `segment`). `check` bắt drift (chưa regenerate).
- **Additive cho qua:** thêm cột `nullable:true` → compat **exit 0** (không over-block).
- **Working tree sạch:** `plan` 0 artifact đổi, `compat` không VỠ.

## Hệ quả

**Dễ hơn:** đổi dữ liệu = PR có (a) drift gate, (b) breaking gate, (c) plan hệ quả — trước khi merge. Đây là
mảnh "plan → gate" của GitOps Pha 7; "apply" idempotent đã có ở các deployer (ADR-0021).

**Khó hơn / phải chấp nhận:**
- `plan` v1 báo **MỚI + SỬA**, chưa báo XOÁ artifact (dataset bị gỡ) — hiếm, và hiện trên diff metadata của
  PR. Tăng sau nếu cần.
- Compat chỉ soi **cột dataset** (schema trên dây). Đổi phá vỡ khác (bỏ topic, đổi PK) chưa gate — increment sau.
- Luật promote encode tay (không gọi thư viện Avro) — đánh đổi lấy nhẹ; đã phủ các kiểu contract đang dùng.

## Phương án đã cân nhắc

- **Sinh `.avsc` + thư viện Avro so compat.** Loại: thêm generator + dependency, mà nguồn sự thật vẫn là
  contract. Encode luật ở contract gọn hơn, đúng nguồn.
- **Chỉ dựa vào Schema Registry chặn lúc runtime.** Loại: muộn — producer đã nổ. Gate ở PR rẻ hơn nhiều.
- **Plan bằng `git diff` thô.** Loại: chỉ thấy YAML, không thấy hệ quả artifact (đúng thứ reviewer cần).
