# Trino — truy vấn liên nguồn

> Một SQL engine, ba nguồn: PostgreSQL (OLTP), ClickHouse (metric), Iceberg (lake).
> Cấu hình: [`trino/etc/catalog/`](../../trino/etc/catalog/) · Thiết kế:
> [ADR-0010](../decisions/0010-trino-for-federation.md).
> Cập nhật lần cuối: 2026-07-15.

---

## 1. Ba catalog

http://localhost:8085 (Web UI) · CLI: `docker exec -it bigdata-trino trino`

| Catalog | Trỏ tới | Dùng để |
|---|---|---|
| `postgres` | `bankdb` — 4 bảng nguồn | Trạng thái **hiện tại**, đúng tới từng giây |
| `clickhouse` | `metrics.*` | Metric đã tổng hợp, TTL 30/90 ngày |
| `iceberg` | `lakehouse.silver.*` qua REST catalog | Lịch sử lake, có snapshot |

Không có catalog cho Elasticsearch hay Bronze/Gold Parquet — chúng chỉ truy cập qua công cụ riêng.

Cả 3 file `.properties` dùng `${ENV:...}` để lấy secret từ biến môi trường của container — không có
mật khẩu trong file.

---

## 2. Khám phá

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM postgres;
SHOW TABLES FROM postgres.public;
SHOW TABLES FROM clickhouse.metrics;
SHOW TABLES FROM iceberg.silver;
DESCRIBE postgres.public.transactions;
```

Chạy một câu rồi thoát:
```bash
docker exec -it bigdata-trino trino --execute "SHOW CATALOGS"
```

---

## 3. Vì sao Trino đáng có ở đây

Giá trị thật là **join xuyên nguồn** — thứ không công cụ nào khác trong stack làm được.

**Đối chiếu OLTP với metric** — số liệu realtime có khớp nguồn không:
```sql
SELECT
  (SELECT COUNT(*) FROM postgres.public.transactions WHERE status = 'completed') AS nguon_oltp,
  (SELECT SUM(success_count) FROM clickhouse.metrics.breakdown
   WHERE window_end = (SELECT MAX(window_end) FROM clickhouse.metrics.breakdown)) AS metric_realtime;
```

**Đối chiếu lake với nguồn** — pipeline batch có mất dòng nào không:
```sql
SELECT
  (SELECT COUNT(*) FROM postgres.public.transactions) AS nguon,
  (SELECT COUNT(*) FROM iceberg.silver.enriched_transactions) AS silver;
```
Chênh lệch là bình thường: Silver bị inner join loại bớt, và chỉ chứa dữ liệu đã tới Bronze.

**Join thật xuyên nguồn** — khách hàng rủi ro cao (Postgres) và hoạt động của họ trong lake (Iceberg):
```sql
SELECT c.customer_id, c.full_name, c.risk_score, COUNT(e.transaction_id) AS so_gd
FROM postgres.public.customers c
JOIN iceberg.silver.enriched_transactions e ON e.customer_id = c.customer_id
WHERE c.risk_score >= 80
GROUP BY c.customer_id, c.full_name, c.risk_score
ORDER BY so_gd DESC
LIMIT 20;
```

---

## 4. Iceberg — snapshot & time travel

Iceberg lộ ra các bảng metadata dạng `"<bảng>$<loại>"`:

```sql
-- Lịch sử snapshot
SELECT * FROM iceberg.silver."enriched_transactions$snapshots";
SELECT * FROM iceberg.silver."enriched_transactions$history";
SELECT * FROM iceberg.silver."enriched_transactions$files";

-- Time travel — đọc trạng thái tại một snapshot cũ
SELECT COUNT(*) FROM iceberg.silver.enriched_transactions FOR VERSION AS OF <snapshot_id>;
SELECT COUNT(*) FROM iceberg.silver.enriched_transactions FOR TIMESTAMP AS OF TIMESTAMP '2026-07-15 09:00:00 UTC';
```

`silver_to_iceberg.py` tạo đúng **2 snapshot**: một từ CTAS, một từ lần append 1000 row. So hai
snapshot là cách nhanh nhất để thấy time travel hoạt động:

```sql
SELECT snapshot_id, committed_at, operation
FROM iceberg.silver."enriched_transactions$snapshots" ORDER BY committed_at;
```

> Job này `DROP TABLE ... PURGE` mỗi lần chạy → `snapshot_id` **đổi hoàn toàn** sau mỗi lần. Đừng lưu
> lại id để dùng về sau.

---

## 5. Vấn đề thường gặp

| Triệu chứng | Nguyên nhân | Xử lý |
|---|---|---|
| Trino không khởi động, lỗi mount | `trino/etc/jvm.config` trên host là **folder** chứ không phải file | Xoá, tạo lại đúng dạng file, `docker compose up -d trino` |
| `SHOW SCHEMAS FROM iceberg` rỗng | Chưa chạy `silver_to_iceberg.py` | [`spark-lakehouse.md`](spark-lakehouse.md) §3.3 |
| `SHOW TABLES FROM clickhouse.metrics` rỗng | Chưa chạy init ClickHouse | [`clickhouse-grafana.md`](clickhouse-grafana.md) §1 |
| Query Postgres chậm | Trino kéo cả bảng về rồi mới lọc | Đưa filter vào `WHERE` để đẩy predicate xuống nguồn |
| Lỗi kết nối catalog | Sai biến môi trường trong `.env` | `docker logs bigdata-trino` |

> Trino **chỉ đọc** trong hệ thống này. Nó không ETL, không ghi. Nó cũng không có auth — hợp lab, không
> hợp production.
