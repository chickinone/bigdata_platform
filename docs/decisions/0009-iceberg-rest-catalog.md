# ADR-0009: Iceberg qua REST catalog, dùng `HadoopFileIO` với MinIO

- **Status:** Accepted
- **Date:** 2026-07-15 *(hồi tố)*
- **Deciders:** Phan Trường

## Bối cảnh

Parquet trần trên S3 (ADR-0008) thiếu những thứ mà một lakehouse thật cần: giao dịch ACID, snapshot,
time travel, schema evolution, và "cái gì là một bảng" được định nghĩa rõ ràng thay vì "cứ mọi file
dưới prefix này".

Iceberg cấp những thứ đó — nhưng cần một **catalog** để theo dõi bảng nào có snapshot nào. Chọn loại
catalog nào?

## Quyết định

Dùng **Iceberg REST catalog** (`tabulario/iceberg-rest:1.6.0`) làm catalog, và ép Spark dùng
**`HadoopFileIO`** thay vì `S3FileIO` khi nói chuyện với MinIO.

```python
.config("spark.sql.catalog.lakehouse.type", "rest")
.config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
.config("spark.sql.catalog.lakehouse.warehouse", "s3a://data-lake-iceberg/warehouse")
.config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
```

## Hệ quả

**Dễ hơn:**
- Snapshot + time travel: `FOR VERSION AS OF`, `FOR TIMESTAMP AS OF`.
- REST catalog **độc lập engine** — cả Spark lẫn Trino đọc cùng bảng qua cùng catalog. Đây chính là
  lý do chọn REST: Trino query được Iceberg mà không cần biết Spark đã ghi thế nào.
- Bảng metadata tra cứu bằng SQL: `"<bảng>$snapshots"`, `"$history"`, `"$files"`.

**Khó hơn / phải chấp nhận:**
- Thêm một service phải chạy (`iceberg-rest`).
- Catalog `tabulario/iceberg-rest` lưu metadata **trong bộ nhớ** — restart là mất đăng ký bảng.
- **Hai biến warehouse với scheme khác nhau**, dễ nhầm là lỗi chính tả:
  ```text
  ICEBERG_WAREHOUSE          = s3a://data-lake-iceberg/warehouse   ← Spark (S3A của Hadoop)
  ICEBERG_CATALOG_WAREHOUSE  = s3://data-lake-iceberg/warehouse    ← iceberg-rest (AWS SDK)
  ```
  Chúng **phải** khác nhau. Spark dùng filesystem S3A; iceberg-rest dùng `S3FileIO` với AWS SDK.

### Vì sao `HadoopFileIO` chứ không `S3FileIO`

Comment trong [`silver_to_iceberg.py`](../../spark/jobs/silver_to_iceberg.py) ghi thẳng lý do:

> *FORCE HadoopFileIO (S3A) thay vì S3FileIO (AWS SDK v2). S3A battle-tested với MinIO, không có
> multipart upload hang.*

`S3FileIO` dùng AWS SDK v2 và **treo ở multipart upload** với MinIO. `HadoopFileIO` đi qua S3A —
đường đã được kiểm chứng. Đây là kinh nghiệm trả bằng thời gian debug, nên đừng "dọn dẹp" nó.

> Config này bị đặt **hai lần** trong file (dòng 22 và 25) — vô hại, nhưng thừa. Dọn khi nào tiện.

### `silver_to_iceberg.py` là job trình diễn

Nó `DROP TABLE ... PURGE` rồi tạo lại từ đầu mỗi lần chạy, sau đó append 1000 row **đọc lại từ chính
Silver** để có snapshot thứ hai mà demo time travel. Nghĩa là:
- Mọi lịch sử snapshot cũ **mất sạch** mỗi lần chạy; `snapshot_id` đổi hoàn toàn.
- 1000 giao dịch bị **nhân đôi** trong bảng.

Đây là code trình diễn, **không** phải pipeline production. Bảng Iceberg thật cần append tăng dần
theo batch, có `MERGE INTO` để upsert, và giữ lịch sử snapshot. Việc đó nằm ở Pha 5 của
[lộ trình](../roadmap/BDP-metadata-driven-roadmap.md).

Bảng cũng **không partition** — comment ghi là để tránh vấn đề bộ nhớ của FanoutWriter. Thêm sau bằng
`ALTER TABLE` nếu cần.

## Phương án đã cân nhắc

- **Hadoop catalog (file-based).** Bị loại: metadata nằm ngay trên S3, không cần service — nhưng
  không an toàn khi nhiều writer, và Trino/Spark phối hợp kém hơn. (Comment sót lại trong code còn
  nhắc "Hadoop catalog, no REST" — di sản từ lần thử đầu.)
- **Hive Metastore.** Bị loại: cần thêm Postgres backend + service HMS. Nặng hơn nhiều so với REST
  catalog cho một lab.
- **`S3FileIO`.** Bị loại: treo multipart upload với MinIO (xem trên).
- **Delta Lake.** Bị loại: Trino hỗ trợ Delta được, nhưng Iceberg có REST catalog chuẩn và tích hợp
  Trino trưởng thành hơn. Iceberg cũng là format phổ biến hơn trong ngành ở thời điểm chọn.
