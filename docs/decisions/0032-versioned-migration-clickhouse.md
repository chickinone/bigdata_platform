# ADR-0032: Migration có version cho ClickHouse — bỏ init-once (Pha 7)

- **Status:** Accepted — runner + ledger + immutability verify trên ClickHouse thật; đóng nợ `notification_events`
- **Date:** 2026-07-20
- **Deciders:** Phan Trường

## Bối cảnh

`clickhouse/init/*.sql` (schema metric sinh từ contract) là **init-once**: chỉ chạy lúc DB mới. DB đang
sống mà đổi contract — thêm cột vào metric cũ, thêm bảng infra — thì `CREATE ... IF NOT EXISTS` **không
đụng bảng đã tồn tại**, nên thay đổi không tới nơi. Không có cách áp thay đổi lên DB sống, không dấu vết
"đã áp gì". Ví dụ nợ thật: `fraud_notifier.py` INSERT vào `metrics.notification_events` nhưng **không init
nào tạo bảng đó** → mọi lần ghi fail âm thầm (nợ 4b ở [BDP-current-state](../architecture/BDP-current-state.md)).

## Quyết định — hai lớp tách bạch, mỗi lớp làm đúng việc

**BASELINE (khai báo)** — giữ `clickhouse/init/*.sql` sinh từ contract, idempotent `CREATE IF NOT EXISTS`.
Là mầm **cài mới**: thêm metric mới = tự có (giữ metadata-driven). Không đổi.

**MIGRATION (mệnh lệnh, versioned)** — `migrations/clickhouse/NNNN_*.sql` + runner
`deployers/clickhouse_migrate.py`. Cho thay đổi INCREMENTAL mà IF NOT EXISTS không làm được: `ALTER ADD
COLUMN`, bảng infra mới, backfill. Runner:
- Ghi mỗi migration đã áp vào `metrics.schema_migrations` (version, name, checksum, applied_at;
  ReplacingMergeTree — một dòng/version).
- Áp **một lần**, theo thứ tự số; chạy lại chỉ áp phần chưa áp (**idempotent**).
- **BẤT BIẾN**: sửa file migration đã áp → checksum lệch → lỗi, buộc tạo migration mới (forward-only).

Kiểm chứng cuối vẫn là verifier `clickhouse_schema` (live vs contract, 0 drift) — gate runtime bắt "đổi
contract mà quên migration".

### Vì sao KHÔNG để runner tự áp cả baseline

Thử cho runner áp lại init mỗi lần thì **treo**: `02_kafka_consumers.sql` có bảng `ENGINE = Kafka`, cần
broker sống. Baseline là việc lúc-dựng-stack (Kafka đã lên); runner chỉ lo lớp evolution — áp được lên DB
sống mà không phụ thuộc Kafka. Tách bạch cũng đúng mô hình chuẩn: declarative desired (init) + imperative
history (migrations).

### Vì sao baseline sinh-từ-contract, migration viết tay

Bảng/cột metric **mới** = init tự sinh (metadata-driven, không cần migration). Chỉ thay đổi mà
`IF NOT EXISTS` bất lực (ALTER bảng cũ, bảng infra ngoài contract như `notification_events`) mới cần
migration mệnh lệnh — bản chất là quyết định người, viết tay + audit được, không suy máy móc từ contract.

## Kiểm chứng (đo thật trên ClickHouse)

- `apply` migration `0001_notification_events` → bảng tạo đúng cột (`rule_type/account_id/severity/action/
  description` + `detected_at`); sổ cái ghi checksum. **Nợ 4b đóng.**
- **Idempotent**: chạy lại → 0 áp.
- **Immutability**: sửa file đã áp → lỗi "checksum lệch, migration bất biến".
- **Verifier** `clickhouse_schema`: 0 lệch (live khớp contract).

## Hệ quả

**Dễ hơn:** áp thay đổi lên DB sống, có dấu vết + idempotent + chống sửa lịch sử. Đóng nợ init-once và
`notification_events`.

**Khó hơn / phải chấp nhận:**
- Đổi CỘT của metric cũ cần **hai bước**: regenerate init (desired, `check`) + thêm migration `ALTER`
  (áp lên DB sống). Verifier `clickhouse_schema` bắt nếu quên bước hai. Chưa tự sinh migration từ diff
  contract (increment sau).
- Cài mới vẫn chạy init riêng (runner không áp baseline). Bootstrap một-lệnh (init khi Kafka sống) để sau.
- Chỉ ClickHouse. **Iceberg**: schema evolution native qua REST catalog (`ALTER TABLE` + snapshot/time-travel)
  — chưa cần runner riêng; làm khi có nhu cầu thật.

## Phương án đã cân nhắc

- **Runner áp cả baseline mỗi lần.** Loại: treo vì bảng Kafka-engine cần broker; và trộn hai mối lo.
- **Full Flyway, baseline đóng băng.** Loại: mất metadata-driven cho bảng mới (mỗi metric mới lại phải viết
  migration tay). Tách baseline-sinh vs migration-tay giữ được cả hai.
- **State-based diff (như Terraform): tự sinh ALTER từ diff contract vs live.** Hoãn: cần diff schema phức
  tạp; verifier `clickhouse_schema` đã bắt được drift, đủ cho giờ.
- **Giữ init-once.** Loại: đúng vấn đề đang gỡ.
