# Migrations — thay đổi schema có version (Pha 7)

> Bỏ "init-once": DB đang sống cũng cập nhật được schema, có theo dõi, idempotent.
> Xem [ADR-0032](../docs/decisions/0032-versioned-migration-clickhouse.md).

## Hai lớp

| Lớp | Ở đâu | Bản chất | Khi nào |
|---|---|---|---|
| **Baseline** (khai báo) | `clickhouse/init/*.sql` (sinh từ contract) | idempotent `CREATE IF NOT EXISTS` | cài mới + bảng metric mới (tự có) |
| **Migration** (mệnh lệnh) | `migrations/clickhouse/NNNN_*.sql` | versioned, bất biến, áp một lần | đổi bảng ĐANG SỐNG: `ALTER ADD COLUMN`, bảng infra mới, backfill |

## Chạy (ClickHouse)

```bash
python -m dataplatform.deployers.clickhouse_migrate plan    # xem migration chờ
python -m dataplatform.deployers.clickhouse_migrate apply   # áp migration chờ (idempotent)
```

Runner ghi mỗi migration đã áp vào `metrics.schema_migrations` (version, name, checksum). Chạy lại chỉ
áp phần chưa áp. **Sửa file migration ĐÃ áp = lỗi** (checksum lệch) — migration bất biến, thay đổi sau
phải tạo file mới.

## Thêm một migration

1. Đặt file `migrations/clickhouse/NNNN_mo_ta.sql` (số thứ tự tăng dần, zero-pad 4 chữ số).
2. Viết DDL mệnh lệnh (`ALTER TABLE ... ADD COLUMN ...`, `CREATE TABLE ...`).
3. `clickhouse_migrate apply`.
4. Kiểm chứng cuối: `python -m dataplatform.verifiers.clickhouse_schema` (live vs contract, 0 lệch).

## Quy ước

- Đánh số `0001`, `0002`, ... — thứ tự áp = thứ tự số.
- Một migration = một thay đổi mạch lạc, có comment *vì sao*.
- **Không sửa migration đã áp.** Sai thì thêm migration sửa lại (forward-only).
- Iceberg: schema evolution native qua REST catalog (`ALTER TABLE` + snapshot) — chưa cần runner riêng
  (xem ADR-0032).
