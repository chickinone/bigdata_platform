# ADR-0028: Lineage cột cho batch Spark — parse SQL bằng sqlglot (Pha 6)

- **Status:** Accepted — sinh xong, `check` 18/18, verify PII flow cấp cột trong `LINEAGE.md`
- **Date:** 2026-07-19
- **Deciders:** Phan Trường

## Bối cảnh

Pha 6 đã có **lineage cột cho Flink** (metric streaming): mỗi cột đầu ra được suy từ `expr` trong
pipeline spec bằng một regex đơn giản (`after.<col>`). Nhưng **Spark batch chưa có** — mà đây mới là
chỗ dữ liệu đi xa nhất: Bronze → Silver (join 3 chiều + dedup CTE) → 3 bảng Gold + Iceberg. Không có
lineage cột Spark thì không trả lời được câu hỏi governance quan trọng nhất: **"cột PII `full_name`
chảy tới những cột nào ở downstream?"**.

Batch spec là **SQL-in-spec** (mô hình dbt, ADR-0024). Muốn lineage cột phải **parse SQL** — kể cả
CTE, subquery, join nhiều bảng, alias, và `SELECT *`.

## Quyết định — dùng sqlglot, không tự viết parser

Trước giờ control plane (`dataplatform/`) cố ý **stdlib-first** (chỉ `PyYAML` + `jsonschema`; OM
deployer còn dùng `urllib` thay `requests` cho nhẹ). Lineage cột Spark là ngoại lệ đáng thêm một
dependency:

- **SQL của Silver không tầm thường**: 2 CTE dedup (`row_number() OVER ... WHERE rn=1`), join 3 chiều,
  cột qualified `t./a./c.`, và rename `c.full_name AS customer_name`. Muốn lần `customer_name` về
  `bronze_customers.full_name` phải **giải qua CTE** — đúng thứ mà một regex/parser tay làm sai.
- **sqlglot có sẵn column-lineage** (`sqlglot.lineage.lineage`) lần được qua CTE/subquery/join. Cấp
  `schema` (tên cột từng input) để nó **expand `SELECT *`** (Iceberg CTAS) và giải cột không định danh bảng.
- Tự viết parser SQL là loại code mong manh, dễ sai âm thầm — trái tinh thần "metadata là nguồn sự thật
  thì phải đúng". sqlglot là công cụ chuẩn, thuần Python, chỉ dùng lúc **sinh artifact** (dev-time), không
  đi vào runtime service nào.

`generators/lineage.py` nay ghép hai nguồn lineage cột — `_flink_column_lineage` (regex như cũ) và
`_spark_column_lineage` (sqlglot) — vào chung `column_lineage`, mỗi bản ghi gắn `engine`. Node id cột
nguồn/đích **quy về đúng node graph** (`_lake_ref` + topic→urn) để khớp với lineage mức bảng.

## Kiểm chứng (đo thật)

Sinh lại (`cli write`) rồi `check`:
- **`check` 18/18** — graph.json + LINEAGE.md đồng bộ metadata.
- **87 bản ghi lineage cột** = 17 Flink + **70 Spark** (5 batch spec).
- **PII cấp cột lần xuyên suốt**: `bank.public.customers.full_name` (PII) → `silver:...customer_name`
  (`c.full_name AS customer_name`, qua CTE) → `gold:customer_lifetime_metrics.customer_name` +
  `gold:high_risk_transactions.customer_name`.
- **Biểu thức tổng hợp giải đúng**: `failed_count ← status` (rút từ `SUM(CASE WHEN status='failed'...)`),
  `lifetime_value ← amount`, `txn_count ← transaction_id`.
- **Iceberg `SELECT *`**: passthrough đủ 17 cột nhờ schema input.

## Hệ quả

**Dễ hơn:** trả lời được "cột PII chảy tới cột nào" **cấp cột, xuyên engine**; thêm/sửa SQL batch thì
lineage tự sinh lại — không bảo trì tay.

**Khó hơn / phải chấp nhận:**
- Thêm **một dev-dependency** (`sqlglot`, chỉ dev-time — không vào runtime). Đã ghi `requirements-dev.txt`;
  CI `metadata-check` cài từ đó.
- Nếu sau này viết SQL mà sqlglot không giải được cột (thiếu schema, hàm lạ), leaf `*` bị **bỏ qua** →
  cột đó hiện "không nguồn cụ thể" thay vì đoán bừa. Suy biến **im lặng nhưng trung thực** — nhìn
  `LINEAGE.md` thấy ngay cột nào cụt.
- Chỉ theo **outer SELECT** của mỗi spec (đúng mô hình một-transform-một-spec hiện tại).

## Phương án đã cân nhắc

- **Tự viết parser nhẹ (regex/tay).** Loại: SQL Silver có CTE + join, regex sẽ giải sai `customer_name`
  về `full_name`. Mong manh, đúng loại nợ kỹ thuật cần tránh.
- **Chạy Spark thật, đọc lineage từ engine (SparkListener/`explain`).** Loại: nặng, cần cụm sống; trái
  triết lý "suy từ metadata, không cần chạy engine".
- **Đẩy luôn lineage cột lên OpenMetadata UI.** Để tăng sau (increment): nay lineage cột nằm ở
  `graph.json`/`LINEAGE.md` — ngang hàng Flink; đẩy `columnsLineage` vào OM là bước kế.
