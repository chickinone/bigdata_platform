# ADR-0006: Một job Flink cho mỗi lane bằng `StatementSet`; `lane1_*.py` rời là di sản

- **Status:** Accepted
- **Date:** 2026-07-15 *(hồi tố)*
- **Deciders:** Phan Trường

> **Cập nhật 2026-07-16 — đã dọn nợ:** 4 file di sản `lane1_{timeseries,kpi,breakdown,topn}.py` **đã
> bị xóa**. Kiểm chứng trước khi xóa: chúng dùng `'connector' = 'print'` (chỉ in console), khác hẳn
> `lane1_dashboard.py` ghi thật vào Kafka `metrics.*`; và không file cấu hình/code nào tham chiếu. Nay
> `flink/jobs/` chỉ còn 2 file (`lane1_dashboard.py`, `lane3_fraud_detection.py`). Phần "Việc còn nợ"
> ở cuối ADR này coi như hoàn thành.

## Bối cảnh

Lane 1 cần 4 metric (`timeseries`, `kpi`, `breakdown`, `topn`) — **tất cả** đều lấy từ cùng một topic
`bankdb.public.transactions`.

Cách viết đầu tiên là 4 file Python độc lập, mỗi file một job Flink. Cách đó nghĩa là **4 job cùng đọc
một topic**, tức 4 lần deserialize Avro cho **cùng** những message đó, 4 consumer group, 4 bộ
checkpoint. Trên cụm 4 slot thì đó là lãng phí lớn.

## Quyết định

Gộp cả 4 metric vào **một** job dùng `StatementSet` của Flink, chia sẻ **một** lần khai báo source.
Bốn file `lane1_*.py` rời trở thành **di sản, không được chạy**.

```python
# Khai báo source một lần, dùng chung cho 4 sink
tenv.execute_sql("CREATE TABLE transactions_source (...)")

stmt_set = tenv.create_statement_set()
stmt_set.add_insert_sql("INSERT INTO timeseries_sink ...")
stmt_set.add_insert_sql("INSERT INTO kpi_sink ...")
stmt_set.add_insert_sql("INSERT INTO breakdown_sink ...")
stmt_set.add_insert_sql("INSERT INTO topn_sink ...")
stmt_set.execute()   # 4 INSERT trong một execution graph
```

## Hệ quả

**Dễ hơn:**
- **Một** lần đọc Kafka, một lần deserialize Avro, dùng chung cho cả 4 metric.
- Một job để theo dõi, một bộ checkpoint, một `group.id` (`flink-lane1-dashboard`).
- Flink tối ưu được toàn bộ 4 nhánh trong cùng một execution graph.

**Khó hơn / phải chấp nhận:**
- **Một metric lỗi làm chết cả 4.** Job là đơn vị fail chung. Đây là đánh đổi có ý thức: đổi khả năng
  cô lập lấy hiệu quả tài nguyên.
- **Bốn file di sản còn nằm trong repo và trông như chạy được.** Đây là cái bẫy thật:
  ```text
  flink/jobs/lane1_timeseries.py   ← đừng chạy
  flink/jobs/lane1_kpi.py          ← đừng chạy
  flink/jobs/lane1_breakdown.py    ← đừng chạy
  flink/jobs/lane1_topn.py         ← đừng chạy
  flink/jobs/lane1_dashboard.py    ← chạy file này
  ```
  Chạy file di sản song song với `lane1_dashboard` → **hai job cùng ghi vào một topic metric**.
  ClickHouse nhận số liệu gấp đôi, và vì bảng dùng `ReplacingMergeTree(inserted_at)` chứ không phải
  khoá nghiệp vụ, bản trùng **không** được gộp một cách đáng tin cậy.
- Khối `ROW<...>` của source vẫn bị lặp giữa `lane1_dashboard.py` và `lane3_fraud_detection.py` — ADR
  này gom được **trong** một lane, không gom được **giữa** các lane. Cần Pha 3 của
  [lộ trình](../roadmap/BDP-metadata-driven-roadmap.md) để giải nốt.

**Việc còn nợ:** xoá 4 file di sản (Pha 0 của lộ trình). Chúng được giữ lại vì có ích khi debug từng
metric riêng, nhưng cái giá là bẫy cho người mới. Hiện cảnh báo ở
[`../guide/flink-jobs.md`](../guide/flink-jobs.md) §1.

## Phương án đã cân nhắc

- **Giữ 4 job riêng.** Bị loại: 4 lần đọc + deserialize cùng dữ liệu, 4 consumer group, 4 bộ
  checkpoint — trên cụm chỉ có 4 slot.
- **Một job cho *mọi* lane** (gộp cả fraud vào). Bị loại: Lane 3 dùng DataStream API với state và timer
  tuỳ biến, không phải Table API. Gộp vào sẽ trộn hai mô hình lập trình và làm một alert lỗi giết chết
  cả dashboard.
- **Gom bằng view chung thay vì StatementSet.** Bị loại: view không giúp gì — mỗi INSERT vẫn thành một
  job riêng nếu không có `StatementSet`.
