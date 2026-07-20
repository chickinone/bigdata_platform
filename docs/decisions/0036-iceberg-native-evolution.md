# ADR-0036: Iceberg dùng schema evolution NATIVE — không migration runner riêng (Pha 7)

- **Status:** Accepted — quyết định + hướng dẫn `migrations/iceberg/README`; native > ledger tự chế
- **Date:** 2026-07-20
- **Deciders:** Phan Trường

## Bối cảnh

Pha 7 đặt "migration có version cho DDL (ClickHouse/Iceberg)". ClickHouse đã có runner + ledger
([ADR-0032](0032-versioned-migration-clickhouse.md)). Câu hỏi: Iceberg có cần runner tương tự?

## Quyết định — KHÔNG dựng runner cho Iceberg

Iceberg (format) đã có tiến hoá schema **versioned SẴN TRONG FORMAT**, mạnh hơn ledger tự chế:

| Runner ClickHouse phải tự làm | Iceberg có native |
|---|---|
| Áp thay đổi lên bảng sống | `ALTER TABLE ADD/DROP/RENAME COLUMN` (Trino/Spark) |
| Sổ cái "đã áp gì" | **snapshot history** — `"<bảng>$history"`, `"$snapshots"`, có `snapshot_id` + `committed_at` |
| Bất biến / audit | snapshot bất biến, nằm trong metadata Iceberg trên S3 (bền) |
| Rollback | **native**: `CALL system.rollback_to_snapshot(...)`, đọc `FOR VERSION AS OF` |
| An toàn đổi tên/thứ tự cột | Iceberg quản schema theo **id cột**, không lệ thuộc vị trí |

Dựng runner + ledger tự chế cho Iceberg là **trùng lặp yếu hơn** native — đúng loại nửa vời cần tránh.
Hướng dẫn dùng native (ALTER + `$history` + rollback_to_snapshot) ở
[`migrations/iceberg/README.md`](../../migrations/iceberg/README.md).

Nếu sau muốn ALTER Iceberg **có review/tự động hoá**, đặt SQL vào `migrations/iceberg/NNNN_*.sql` chạy qua
Trino — nhưng ledger vẫn đọc từ `$history` native, KHÔNG tự chế bảng tracking (iceberg-rest lưu catalog
trong RAM, bảng tự chế mất khi restart; snapshot nằm trên S3, bền hơn — bài học ADR-0009/0025).

## Hệ quả

**Dễ hơn:** không thêm code phải bảo trì; audit + rollback dữ liệu mạnh hơn ClickHouse (time-travel).

**Khó hơn / phải chấp nhận:**
- Hai engine, hai cơ chế "migration" (CH: runner tự chế; Iceberg: native) — khác nhau vì bản chất format
  khác. Ghi rõ để không nhầm.
- ALTER Iceberg hiện chạy tay qua Trino; tự động hoá có review là increment sau nếu cần.

## Phương án đã cân nhắc

- **Runner + ledger cho Iceberg (như ClickHouse).** Loại: trùng lặp yếu hơn native; ledger tự chế còn dễ
  mất (catalog RAM).
- **Bỏ qua hoàn toàn.** Loại: vẫn cần TÀI LIỆU hoá cơ chế native để đội biết cách evolve + rollback.
