# ADR-0031: Airflow DAG sinh từ metadata — orchestration batch (Pha 7)

- **Status:** Accepted — generator + `check` 19/19; DAG construct + đồ thị phụ thuộc verify (stub Airflow)
- **Date:** 2026-07-20
- **Deciders:** Phan Trường

## Bối cảnh

Batch medallion (silver → 3 gold + iceberg) tới nay chạy bằng deployer `spark_batch apply` — submit tuần
tự theo `_stage`, chạy tay khi cần. Thiếu **orchestration thật**: lịch, retry, SLA, theo dõi, và một
sơ đồ phụ thuộc để nhìn. Pha 7 cần đưa nó vào một scheduler (Airflow).

Cạm bẫy kinh điển: **DAG viết tay tách rời khỏi định nghĩa pipeline**. Thêm một job gold mà quên nối
phụ thuộc trong DAG → nó chạy trước silver → đọc data cũ/rỗng, âm thầm sai. Đây đúng loại sprawl mà cả
dự án đang diệt.

## Quyết định — sinh DAG từ phụ thuộc, không viết tay

`generators/airflow_dag.py` sinh `airflow/dags/medallion_batch_dag.py`. **Thứ tự task suy từ phụ thuộc
input/output của batch spec** — job đọc `data-lake-silver` phụ thuộc job GHI ra nó. Cùng quan hệ mà
`_batch_edges` (lineage) và `_stage` (spark_batch) đã dùng, nay thành cạnh DAG:

```
silver_enriched_transactions ──▶ [gold_customer_lifetime_metrics, gold_daily_transaction_summary,
                                   gold_high_risk_transactions, iceberg_silver_enriched]
```

Thêm/sửa batch spec = DAG tự đổi task + cạnh; **không đụng file DAG**. Cắm vào `_collect()` nên `check`
giữ DAG khớp metadata (byte-exact, 18→**19** artifact) — sửa tay DAG là CI đỏ.

### Một nguồn sự thật cho "chạy job thế nào"

Task DAG chạy ĐÚNG lệnh `docker exec bigdata-spark-master spark-submit ...` như deployer, qua hàm chung
`spark_batch.submit_argv(spec)` (tách ra lúc này, deployer + DAG cùng gọi). Không có "đường thứ hai" để
lệch — DAG chạy y hệt chạy tay.

### Ingestion CDC không nằm trong DAG

Debezium → Bronze là **stream chạy liên tục**, không phải task batch có điểm bắt đầu/kết thúc. Nên nó là
thượng nguồn NGẦM của silver (silver đọc Bronze), không phải một node DAG. Đưa stream vào DAG batch là sai
mô hình (sensor chờ Bronze là increment sau nếu cần).

## Kiểm chứng

- **`check` 19/19 byte-exact** — DAG sinh khớp file trên đĩa.
- **DAG construct được + đồ thị đúng**: exec file DAG với **stub Airflow** (không cần cài Airflow) —
  5 task dựng được, `silver` là root, 4 leaf phụ thuộc đúng silver, **không chu trình**. `ast.parse` OK.
- **Compose runtime hợp lệ** (`airflow/docker-compose-airflow.yml`, `docker compose config` pass).

Bật Airflow thật để xem DAG chạy trên UI là **phiên riêng** (cần stack chính + RAM) — như OM/Trino trong
dự án này. Scaffolding sẵn: [`airflow/README.md`](../../airflow/README.md).

## Hệ quả

**Dễ hơn:** orchestration có lịch/retry/SLA; sơ đồ phụ thuộc nhìn được; thêm job = sửa spec, DAG tự theo.

**Khó hơn / phải chấp nhận:**
- Airflow là **phiên riêng** trên máy này (RAM). Standalone (SequentialExecutor + SQLite) đủ cho lab,
  không production.
- `schedule/retries/sla` là mặc định DAG khai trong generator (một chỗ) — chưa cho override per-pipeline;
  thêm khi cần (khối `orchestration` trong spec).
- Task dùng `BashOperator` + `docker exec` (cần docker CLI + socket trong container Airflow) thay vì
  `SparkSubmitOperator` — đổi lấy "chạy y hệt deployer đã chứng minh", không dựng lại đường submit.

## Phương án đã cân nhắc

- **DAG viết tay.** Loại: tách rời định nghĩa pipeline, đúng sprawl đang diệt.
- **`SparkSubmitOperator`.** Loại (hiện tại): phải cấu hình lại connection Spark trong Airflow + đường submit
  thứ hai; `docker exec` tái dùng đúng lệnh đã verify. Cân nhắc lại nếu bỏ mô hình docker-compose.
- **Dagster.** Cân nhắc: asset-based hợp "sinh từ lineage" hơn. Chọn Airflow vì phổ biến + người đang học;
  có thể đổi sau, generator là lớp cô lập.
- **Đưa CDC/stream vào DAG.** Loại: stream liên tục, không phải task batch — sai mô hình.
