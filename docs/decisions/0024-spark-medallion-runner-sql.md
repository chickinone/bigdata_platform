# ADR-0024: Spark medallion runner — transform bằng SQL (mô hình dbt)

- **Status:** Accepted (Silver đã cắt chuyển); Gold + Iceberg theo cùng khuôn
- **Date:** 2026-07-18
- **Deciders:** Phan Trường

## Bối cảnh

Ba job Spark viết tay: `enrich_transactions.py` (Silver: dedup + join 3 chiều), `build_gold_layer.py`
(Gold: 3 bảng — 2 aggregation + 1 filter), `silver_to_iceberg.py` (phần lớn là demo dạy học). Sprawl #10
(cột join/output Silver ẩn trong code) và #11 (không ai biết Gold phụ thuộc cột nào). Thêm bảng lake =
viết Python mới.

## Quyết định — SQL trong spec (mô hình dbt), KHÔNG cấu-trúc-hoàn-toàn

Với Flink metric (4 cái **cùng khuôn** windowed aggregation) đã chọn *cấu trúc khai báo hoàn toàn*
([ADR-0023](0023-flink-metric-runner-declarative.md)). Spark medallion **khác khuôn** (dedup + join +
agg + filter trộn nhau), nên chọn khác — và đây là chỗ cần nói rõ vì sao **không** mâu thuẫn:

> "Cấu trúc hoàn toàn" **không** metadata-driven hơn cho ETL khác khuôn. Những thứ như
> `SUM(when status='failed')`, `countDistinct`, điều kiện join 3 chiều **bản thân là biểu thức** — chẻ
> ra chỉ rải SQL fragment khắp nhiều field, cộng generator phình + rủi ro parity cao. Cái mang lại giá
> trị metadata-driven là **schema output + path/partition + phụ thuộc được khai + sinh**, không phải việc
> chẻ transform.

**Bằng chứng ngành:** `dbt` — công cụ metadata-driven thành công nhất cho tầng này — dùng đúng mô hình
**SQL SELECT (transform) + YAML (contract)**. Cấu-trúc-hoàn-toàn là mô hình *semantic layer* (LookML/Cube),
hợp cho metric đồng khuôn, không hợp cho ETL. Chọn đúng công cụ cho đúng tầng là trưởng thành kiến trúc.

### Hình dạng

- **Batch spec** (`metadata/pipelines/batch/*.yaml`) tự chứa như một dbt model: `inputs` (parquet →
  view), `sql` (transform), `output` (path/format/partition + **columns** = hợp đồng schema, diệt #10/#11).
- **Runner mỏng** (`spark/jobs/medallion_runner.py`): đọc input thành view → chạy SQL → ghi theo output.
  KHÔNG chứa logic. Sinh trên host (job plan JSON), thực thi trong container — container không cần pyyaml.
- **Deployer** (`deployers/spark_batch.py`, `plan`/`apply`): sinh job plan + `spark-submit` **theo thứ tự
  layer** (silver trước gold vì gold đọc silver).

## Kiểm chứng — parity với job cũ (Silver)

Chạy job CŨ `enrich_transactions.py` làm **baseline**, rồi runner MỚI, so:

```
enrich_transactions.py (cũ):        Enriched transactions: 72
medallion_runner (silver, sinh):    WROTE 72 rows -> data-lake-silver/enriched_transactions/
```

**72 = 72** — row count khớp tuyệt đối. SQL trong spec là bản dịch trung thành từng dòng của DataFrame cũ
(cùng 3 join, cùng dedup `row_number` theo PK+`updated_at`, cùng 14 cột + 3 cột phân vùng = 17), nên schema
khớp theo cấu tạo. Output phân vùng `year/month/day` — hiện rơi vào `__HIVE_DEFAULT_PARTITION__` vì
`posted_at` NULL ở dữ liệu test (giao dịch chưa "posted"); job cũ cho **y hệt**, nên đây là đặc tính dữ
liệu, không phải lệch.

## Hệ quả

**Dễ hơn:** thêm bảng lake = 1 batch spec (contract + SQL), không Python. Schema output + phụ thuộc
(inputs) khai tường minh → hết cảnh "không biết Silver/Gold phụ thuộc cột gì" (#10/#11).

**Khó hơn / phải chấp nhận:**
- Transform là SQL (authored), không sinh 100% — đúng mô hình dbt, đánh đổi có chủ ý cho ETL khác khuôn.
- `silver_to_iceberg.py` phần lớn là **demo** (time-travel/schema-evolution dạy học) — sẽ tách: sinh phần
  `CREATE TABLE AS SELECT` từ contract, giữ demo như tài liệu riêng (chưa làm ở ADR này).
- Job plan là artifact **runtime** → không commit, sinh lúc deploy.
- Fix vận hành: `--conf spark.jars.ivy=/tmp/.ivy2` (thư mục ivy mặc định không ghi được khi container fresh).

## Việc còn lại

- ✅ Silver cắt chuyển (`enrich_transactions.py` xoá).
- ⬜ **Gold** (3 bảng) — cùng runner, chỉ thêm 3 batch spec; parity với `build_gold_layer.py` rồi xoá nó.
- ⬜ **Iceberg** — sinh CTAS từ contract Silver (tách khỏi demo).
- ⬜ Verifier schema Silver/Gold vs `output.columns` (như verifier ClickHouse, ADR-0022) — Pha 6/7.

## Phương án đã cân nhắc

- **Cấu trúc khai báo hoàn toàn** (như Flink): loại — không metadata-driven hơn cho ETL khác khuôn, generator
  phình + rủi ro parity cao. Mô hình dbt (SQL) là chuẩn ngành cho tầng này.
- **Lai (cấu trúc + SQL)**: loại — hai đường code, phức tạp thừa khi SQL đã phủ hết đồng nhất.
- **Giữ transform trong Python**: loại — đó chính là sprawl #10/#11 (phụ thuộc ẩn trong code).
