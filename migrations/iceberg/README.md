# Iceberg — schema evolution NATIVE (không runner riêng)

> Quyết định (ADR-0036): Iceberg **không** cần migration runner như ClickHouse. Format Iceberg đã có
> tiến hoá schema versioned SẴN, mạnh hơn một ledger tự chế.

## Vì sao không dựng runner

| Thứ migration runner tự chế cho | Iceberg đã có sẵn |
|---|---|
| Áp thay đổi lên DB sống | `ALTER TABLE ... ADD/DROP/RENAME COLUMN` (qua Trino/Spark) |
| Sổ cái "đã áp gì" | **snapshot history** — mọi thay đổi là một snapshot, xem `"<bảng>$history"`, `"$snapshots"` |
| Bất biến / audit | snapshot bất biến, có `committed_at` + `snapshot_id` |
| Rollback | **native**: `CALL system.rollback_to_snapshot(...)` / đọc `FOR VERSION AS OF <id>` |
| Chống lệch schema | Iceberg quản schema theo id cột, an toàn khi đổi tên/thứ tự |

Dựng thêm runner + ledger cho Iceberg là **trùng lặp yếu hơn** native — đúng loại nửa vời cần tránh.

## Cách "migrate" một bảng Iceberg

Chạy qua Trino (hoặc Spark) — mỗi câu tạo một snapshot mới, có lịch sử:

```sql
-- thêm cột (backward-safe)
ALTER TABLE iceberg.silver.enriched_transactions ADD COLUMN segment VARCHAR;

-- xem lịch sử thay đổi (sổ cái native)
SELECT made_current_at, snapshot_id, operation
FROM iceberg.silver."enriched_transactions$history" ORDER BY made_current_at;

-- rollback về snapshot trước nếu cần
CALL iceberg.system.rollback_to_snapshot('silver', 'enriched_transactions', <snapshot_id>);
```

## Khi nào cần đặt file ở đây

Nếu sau này muốn ALTER Iceberg **có review + tự động hoá** (không gõ tay trên Trino), đặt SQL vào
`migrations/iceberg/NNNN_*.sql` và chạy qua Trino trong CI/deploy. Ledger vẫn nên đọc từ `$history`
native, không tự chế bảng tracking (iceberg-rest lưu catalog trong RAM — bảng tracking tự chế sẽ mất khi
restart; snapshot history nằm trong metadata Iceberg trên S3, bền hơn).
