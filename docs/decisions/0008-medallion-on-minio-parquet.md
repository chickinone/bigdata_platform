# ADR-0008: Medallion (Bronze/Silver/Gold) dạng Parquet trên MinIO

- **Status:** Accepted
- **Date:** 2026-07-15 *(hồi tố)*
- **Deciders:** Phan Trường

## Bối cảnh

Streaming trả lời "đang xảy ra chuyện gì" trong cửa sổ vài phút. Nhưng có những câu hỏi nó không trả
lời được: tổng hợp toàn lịch sử, join qua nhiều thực thể, chạy lại trên dữ liệu cũ. Cần một tầng lưu
trữ và một cách xử lý batch.

Câu hỏi kèm theo: ranh giới giữa streaming và batch nằm ở đâu?

## Quyết định

Áp dụng **kiến trúc medallion** trên MinIO dạng Parquet, với Kafka Connect làm chặng ghi Bronze và
Spark làm mọi biến đổi sau đó.

| Tầng | Đường dẫn | Ai ghi | Nội dung |
|---|---|---|---|
| **Bronze** | `s3a://data-lake-bronze/topics/<topic>/year=/month=/day=/hour=` | S3 sink connector | Raw CDC đã `unwrap`, partition theo giờ |
| **Silver** | `s3a://data-lake-silver/enriched_transactions/` | `enrich_transactions.py` | Đã dedup + join, partition `year/month/day` |
| **Gold** | `s3a://data-lake-gold/<bảng>/` | `build_gold_layer.py` | 3 bảng tổng hợp cho báo cáo |

**Ranh giới:** Kafka Connect ghi Bronze và **không** biến đổi gì (ngoài `unwrap`). Mọi logic nghiệp vụ
nằm ở Spark. Kafka Connect không join, không tổng hợp.

## Hệ quả

**Dễ hơn:**
- **Bronze là bất biến và replay được.** Sai logic ở Silver? Sửa Spark, chạy lại — Bronze không đổi.
- Mỗi tầng có một trách nhiệm rõ: Bronze = độ trung thực, Silver = tính đúng đắn, Gold = tiện dụng.
- Parquet đọc được bởi Spark, Trino, DuckDB, pandas — không khoá vào công cụ nào.
- `timestamp.extractor=Record` → replay cho ra **cùng** layout partition, không phụ thuộc lúc chạy.

**Khó hơn / phải chấp nhận:**
- **`unwrap` làm Bronze mất `op` và `ts_ms`.** File Bronze trông như bảng nguồn, không như CDC log.
  Spark **không phân biệt được INSERT với UPDATE** — nên phải dedup bằng `updated_at` (xem
  [`../architecture/BDP-lakehouse-medallion.md`](../architecture/BDP-lakehouse-medallion.md) §3).
  Đây là đánh đổi thật: Bronze dễ đọc hơn, nhưng mất ngữ nghĩa CDC.
- **Silver là full refresh** (`mode("overwrite")`) — mỗi lần chạy đọc lại **toàn bộ** Bronze. Đúng cho
  lab, không mở rộng được.
- **Inner join loại dòng âm thầm.** Giao dịch có account chưa tới Bronze sẽ biến mất khỏi Silver, không
  có bảng lỗi nào ghi lại.
- **Không có orchestrator.** Ba job Spark phải chạy tay theo đúng thứ tự. Chạy Gold khi chưa có Silver
  → fail. Đây là Pha 7 của [lộ trình](../roadmap/BDP-metadata-driven-roadmap.md).
- **Bucket không được tạo tự động.** `minio-init` chỉ tạo `flink-checkpoints`/`flink-savepoints`.
- Parquet trần **không có** ACID, không time travel, không schema evolution — lý do có
  [ADR-0009](0009-iceberg-rest-catalog.md).

## Phương án đã cân nhắc

- **Flink ghi thẳng lake.** Bị loại: Flink StreamingFileSink tạo nhiều file nhỏ và cần rollover
  policy cẩn thận. Kafka Connect S3 sink được sinh ra đúng cho việc này (`flush.size` +
  `rotate.interval.ms`), và giữ Flink tập trung vào tính toán.
- **Không `unwrap`, giữ nguyên envelope CDC.** Bị cân nhắc nghiêm túc — sẽ giữ được `op`/`ts_ms` và
  cho Silver phân biệt INSERT/UPDATE. Bị loại vì Bronze khi đó lồng sâu (`after.transaction_id`) và
  mọi query Spark phải bóc tách. **Có thể nên xem lại**: cái giá là dedup bằng `updated_at` thay vì
  dùng ngữ nghĩa CDC thật.
- **Bỏ Silver, Bronze → Gold thẳng.** Bị loại: mọi job Gold sẽ phải tự lặp lại logic dedup + join.
- **Ghi Bronze theo JSON.** Bị loại: Parquet nén tốt hơn nhiều và có cột — Spark chỉ đọc cột cần.
