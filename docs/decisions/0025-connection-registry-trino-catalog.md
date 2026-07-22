# ADR-0025: Connection registry + sinh Trino catalog — mở Pha 6

- **Status:** Accepted — sinh + oracle byte-exact + federation runtime (postgres/clickhouse) đã verify
- **Date:** 2026-07-19
- **Deciders:** Phan Trường

## Bối cảnh

Hai khoảng trống gặp nhau ở đây:
- **Nợ Pha 1:** dataset tham chiếu `source.connection: postgres_main`, nhưng **không có registry** định
  nghĩa `postgres_main` là gì. "Connection" chỉ là một chuỗi tên treo lơ lửng.
- **Sprawl #13 (Pha 6):** `trino/etc/catalog/*.properties` viết tay, tách rời khỏi định nghĩa connection.
  Đổi host/cổng một nguồn = sửa nhiều nơi.

## Quyết định

Thêm **connection registry** (`metadata/connections/*.yaml`): mỗi hệ thống kết nối là một contract
(`name`, `type`, mô tả) — đóng nợ Pha 1. Connection nào Trino query được thì mang thêm khối `trino`
(catalog + connector + properties); generator `trino_catalog.py` sinh `trino/etc/catalog/<catalog>.properties`
từ đó — đóng sprawl #13.

Secret **không** nằm trong metadata: mọi giá trị nhạy cảm là `${ENV:...}`, Trino tự resolve (cùng nguyên
tắc với connector JSON / deployer).

```yaml
# metadata/connections/postgres_main.yaml
name: postgres_main
type: postgres
trino:
  catalog: postgres
  connector: postgresql
  properties:
    connection-url: "jdbc:postgresql://${ENV:POSTGRES_HOST}:${ENV:POSTGRES_PORT}/${ENV:POSTGRES_DB}"
    connection-user: "${ENV:POSTGRES_USER}"
    connection-password: "${ENV:POSTGRES_PASSWORD}"
```

### Vì sao carry `properties` trực tiếp (không mô hình hoá sâu)

Cùng lý luận như Spark medallion (ADR-0024): property của Trino catalog là **key-value đặc thù từng
connector** (iceberg có `hive.s3.*`, postgres có `connection-url`...) — không đồng khuôn. Mô hình hoá sâu
sẽ đẻ ra bảng ánh xạ khổng lồ + cửa thoát hiểm. Carry map trực tiếp là đúng: cái mang giá trị metadata là
connection thành **registry hạng nhất** (dataset tham chiếu được, catalog sinh từ nó, sau này DataHub/
lineage đọc nó), không phải việc chẻ nhỏ property.

## Kiểm chứng — oracle byte-exact

`check` so catalog sinh với 3 file viết tay: **3/3 khớp tuyệt đối** → `check` giờ **16/16**.

```
[KHỚP] trino/etc/catalog/clickhouse.properties
[KHỚP] trino/etc/catalog/iceberg.properties
[KHỚP] trino/etc/catalog/postgres.properties
```

Vì sinh == viết tay từng byte, và Trino đọc đúng file đó, việc sinh là **trong suốt** với runtime.

**Federation runtime đã verify đủ 3 nguồn** (cập nhật 2026-07-20) — một câu Trino chạm cả ba engine:
```
postgres.public.transactions              = 1046   (khớp Postgres)
clickhouse.metrics.timeseries             = 7      (khớp ClickHouse)
iceberg.silver.enriched_transactions      = 1072   (khớp Spark ghi)
```
Cả join chéo engine (`iceberg ⋈ postgres ON transaction_id`) lẫn đọc cột thật + aggregate trên iceberg
đều chạy, không treo.

> **Nợ "iceberg query treo" đã đóng.** Phiên trước treo là **sự cố Docker nhất thời**, không phải lỗi
> config: `SELECT`/`COUNT`/CTAS trên iceberg nay chạy sạch với đúng `iceberg.properties` sinh từ registry.
> **Bẫy vận hành cần nhớ** (khớp [ADR-0009](0009-iceberg-rest-catalog.md)): `tabulario/iceberg-rest` lưu
> catalog **trong RAM** — restart là `namespaces:[]` rỗng, bảng "biến mất" (dễ nhầm là treo/lỗi). Phải chạy
> lại Spark iceberg job để đăng ký lại bảng trước khi Trino query.

## Hệ quả

**Dễ hơn:** connection là nguồn sự thật duy nhất. Thêm nguồn Trino = thêm khối `trino` vào connection,
không sửa `.properties` tay. Nợ Pha 1 (mã hoá connection) đóng cho các connection Trino.

**Khó hơn / phải chấp nhận:**
- ~~Mới encode 3 connection có Trino; kafka/es/s3/schema-registry chưa vào registry.~~ **đã đóng**
  ([ADR-0029](0029-encode-connection-non-trino.md)): 4 connection non-Trino nay vào registry, generator đọc
  endpoint từ đó thay vì hardcode. Nợ Pha 1 (mã hoá connection) đóng hoàn toàn.
- [x] ~~Runtime Trino chưa verify.~~ Federation 3 nguồn đã verify (xem Kiểm chứng ở trên).

## Việc còn lại của Pha 6

- [x] Verify Trino federation runtime (query chéo postgres × clickhouse × iceberg) — **Xong** (2026-07-20).
- [x] **Lineage cột-tới-cột** — Flink (regex) + Spark (sqlglot, [ADR-0028](0028-spark-column-lineage-sqlglot.md)).
- [x] **Catalog chuẩn ngành** (OpenMetadata): schema, ownership, tag PII, lineage cột từ contract
  ([ADR-0027](0027-openmetadata-catalog.md)).

**→ Pha 6 đóng.** Ba câu hỏi discovery/lineage/federation đều trả lời được từ metadata + verify runtime.

## Phương án đã cân nhắc

- **Mô hình hoá sâu property Trino thành field.** Bị loại: property đặc thù connector, không đồng khuôn —
  carry map trực tiếp (như ADR-0024).
- **Giữ `.properties` viết tay.** Bị loại: đó là sprawl #13, tách rời khỏi connection.
- **Nhét secret vào connection contract.** Bị loại dứt khoát: `${ENV:...}`, không bao giờ giá trị thật.
