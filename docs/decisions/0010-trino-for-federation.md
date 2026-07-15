# ADR-0010: Trino làm lớp truy vấn liên nguồn (chỉ đọc)

- **Status:** Accepted
- **Date:** 2026-07-15 *(hồi tố)*
- **Deciders:** Phan Trường

## Bối cảnh

Dữ liệu nằm rải ở ba nơi với ba giao diện truy vấn khác nhau: PostgreSQL (trạng thái OLTP hiện tại),
ClickHouse (metric đã tổng hợp), Iceberg trên MinIO (lịch sử lake).

Nhiều câu hỏi hữu ích lại **cắt ngang** các nguồn đó:
- "Metric realtime có khớp với nguồn OLTP không?" → ClickHouse + Postgres
- "Pipeline batch có mất dòng nào không?" → Postgres + Iceberg
- "Khách hàng rủi ro cao (Postgres) đã giao dịch gì (Iceberg)?" → join thật xuyên nguồn

Không có Trino, mỗi câu hỏi như vậy là một script export-rồi-so-tay.

## Quyết định

Thêm **Trino** làm lớp query federation **chỉ đọc**, với 3 catalog: `postgres`, `clickhouse`,
`iceberg`. Trino **không** ETL và **không** ghi.

```properties
# trino/etc/catalog/postgres.properties
connector.name=postgresql
connection-url=jdbc:postgresql://${ENV:POSTGRES_HOST}:${ENV:POSTGRES_PORT}/${ENV:POSTGRES_DB}
```

Cả 3 file dùng `${ENV:...}` — không có mật khẩu trong file cấu hình.

## Hệ quả

**Dễ hơn:**
- Một phương ngữ SQL cho cả ba nguồn; join xuyên nguồn được.
- Đối chiếu tầng này với tầng kia bằng một câu query — công cụ tốt nhất để **kiểm chứng** pipeline.
- Iceberg time travel qua SQL: `FOR VERSION AS OF`, và bảng `"$snapshots"`.
- Thêm nguồn = thêm một file `.properties`.

**Khó hơn / phải chấp nhận:**
- **Bẫy mount `jvm.config`.** File này mount **kiểu file**, không phải thư mục. Chưa có trên host thì
  Docker tạo một **folder** trùng tên và Trino chết với *"Are you trying to mount a directory onto a
  file or vice-versa?"* — lỗi hay gặp nhất trên Windows. Xử lý:
  [`../guide/trino.md`](../guide/trino.md) §5.
- **Không auth.** Ai vào được `:8085` là đọc được mọi thứ, gồm cả bảng có PII. Chấp nhận cho lab, chặn
  ở production.
- **Predicate pushdown không phải lúc nào cũng có.** Query Postgres không có `WHERE` chọn lọc sẽ kéo cả
  bảng về Trino rồi mới lọc.
- **Catalog là metadata sprawl #13** — connection string lặp lại thứ đã có trong compose. Pha 6 của
  [lộ trình](../roadmap/BDP-metadata-driven-roadmap.md) sinh chúng từ connection registry.
- Không có catalog cho **Elasticsearch** hay **Bronze/Gold Parquet** — chỉ Iceberg mới query được từ
  Trino. Đây là khoảng trống, không phải quyết định.

## Phương án đã cân nhắc

- **Trino cũng làm ETL.** Bị loại rõ ràng. Trino giỏi query liên nguồn, không phải engine biến đổi.
  Spark đã giữ vai đó. Một công cụ một nhiệm vụ.
- **Presto.** Bị loại: Trino là nhánh được phát triển tích cực hơn, connector Iceberg tốt hơn.
- **Chỉ dùng Spark cho query ad-hoc.** Bị loại: khởi động session Spark cho một câu query đối chiếu là
  quá nặng; Spark cũng không nói chuyện với ClickHouse gọn như Trino.
- **Thêm catalog cho Bronze/Gold qua Hive connector.** Bị hoãn: cần Hive Metastore. Bronze/Gold hiện
  chỉ Spark đọc.
