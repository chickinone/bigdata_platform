# ADR-0025: Connection registry + sinh Trino catalog — mở Pha 6

- **Status:** Accepted — sinh + oracle byte-exact + federation runtime (postgres/clickhouse) đã verify
- **Date:** 2026-07-19
- **Deciders:** Phan Trường

## Bối cảnh

Hai khoảng trống gặp nhau ở đây:
- **Nợ Pha 1:** dataset tham chiếu `source.connection: postgres_main`, nhưng **không có registry** định
  nghĩa `postgres_main` LÀ gì. "Connection" chỉ là một chuỗi tên treo lơ lửng.
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

`check` so catalog sinh với 3 file viết tay: **3/3 KHỚP tuyệt đối** → `check` giờ **16/16**.

```
[KHỚP] trino/etc/catalog/clickhouse.properties
[KHỚP] trino/etc/catalog/iceberg.properties
[KHỚP] trino/etc/catalog/postgres.properties
```

Vì sinh == viết tay từng byte, và Trino đọc đúng file đó, việc sinh là **trong suốt** với runtime.

**Federation runtime đã verify:** `SHOW CATALOGS` liệt kê cả 3 catalog sinh; query thật trả đúng dữ liệu:
```
postgres.public.transactions        = 1046   (khớp Postgres)
clickhouse.metrics.timeseries       = 7      (khớp ClickHouse)
```
Iceberg catalog **load được** nhưng query treo — vấn đề runtime Trino↔iceberg-rest↔MinIO (độc lập với file
catalog, vì nó byte-exact bản cũ). Ghi nhận là nợ runtime riêng, không phải lỗi generation.

## Hệ quả

**Dễ hơn:** connection là nguồn sự thật duy nhất. Thêm nguồn Trino = thêm khối `trino` vào connection,
không sửa `.properties` tay. Nợ Pha 1 (mã hoá connection) đóng cho các connection Trino.

**Khó hơn / phải chấp nhận:**
- Mới encode 3 connection có Trino (postgres, clickhouse, iceberg). Các connection khác (kafka, es, s3,
  schema-registry) chưa vào registry — thêm khi cần (chúng không sinh Trino catalog, chỉ là registry entry).
- Runtime Trino chưa verify (Docker bất ổn phiên này) — nhưng byte-exact nên rủi ro thấp.

## Việc còn lại của Pha 6

- ⬜ Verify Trino federation runtime (query chéo postgres × clickhouse × iceberg).
- ⬜ **Lineage cột-tới-cột** (suy từ pipeline spec: `source_urn`→`sink_urn` + SQL; parse logical plan Spark).
- ⬜ **Catalog chuẩn ngành** (OpenMetadata / DataHub): ingest schema, ownership, tag PII từ contract. Cần
  hạ tầng mới — increment riêng.

## Phương án đã cân nhắc

- **Mô hình hoá sâu property Trino thành field.** Bị loại: property đặc thù connector, không đồng khuôn —
  carry map trực tiếp (như ADR-0024).
- **Giữ `.properties` viết tay.** Bị loại: đó là sprawl #13, tách rời khỏi connection.
- **Nhét secret vào connection contract.** Bị loại dứt khoát: `${ENV:...}`, không bao giờ giá trị thật.
