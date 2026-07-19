# Airflow — orchestration batch medallion (Pha 7)

> DAG **sinh từ metadata**, không viết tay: [`dags/medallion_batch_dag.py`](dags/medallion_batch_dag.py)
> sinh bởi `dataplatform/generators/airflow_dag.py`. Thứ tự task suy từ phụ thuộc input/output của batch
> spec (silver đọc Bronze → gold/iceberg đọc silver). Xem [ADR-0031](../docs/decisions/0031-airflow-dag-from-metadata.md).

## DAG có gì

5 task Spark medallion, phụ thuộc suy tự động:

```
silver_enriched_transactions  ─┬─▶ gold_customer_lifetime_metrics
                               ├─▶ gold_daily_transaction_summary
                               ├─▶ gold_high_risk_transactions
                               └─▶ iceberg_silver_enriched
```

Mỗi task chạy ĐÚNG lệnh `docker exec bigdata-spark-master spark-submit ...` như deployer `spark_batch`
(qua `submit_argv` — một nguồn sự thật). Ingestion CDC (Debezium→Bronze) là stream chạy liên tục, không
phải task batch → là thượng nguồn ngầm của silver, không nằm trong DAG.

## Sinh lại DAG khi metadata đổi

```bash
python -m dataplatform.cli write     # sinh lại dags/medallion_batch_dag.py
python -m dataplatform.cli check      # 19/19 — DAG khớp metadata (CI gate)
```

Thêm/sửa batch spec = DAG tự đổi task + phụ thuộc. **Đừng sửa file DAG bằng tay** (CI `check` sẽ đỏ).

## Chạy (phiên RIÊNG — cần stack chính đang chạy)

Task DAG gọi `docker exec` vào `bigdata-spark-master`, nên **stack chính (spark-master, minio, iceberg-rest)
phải đang chạy**. Trên máy 15GB: dừng OpenMetadata trước nếu cần RAM.

```bash
docker compose up -d spark-master spark-worker minio iceberg-rest    # thượng nguồn
docker compose -f airflow/docker-compose-airflow.yml up -d           # Airflow standalone
# UI: http://localhost:8090  (user 'admin', mật khẩu in trong log lần đầu:
#   docker logs bigdata-airflow 2>&1 | grep -i password )
```

Bật DAG `medallion_batch` trên UI rồi Trigger — hoặc chạy một lần:

```bash
docker exec bigdata-airflow airflow dags trigger medallion_batch
```

## Ghi chú

- `airflow standalone` = SequentialExecutor + SQLite trong một tiến trình. Đủ cho lab, **không** production
  (production: CeleryExecutor/KubernetesExecutor + Postgres metadata + scheduler/web tách rời).
- Container tự `apt install docker.io` lúc khởi động để có docker CLI (BashOperator cần) + mount `docker.sock`.
- `schedule="@daily"`, `retries=2`, `sla=2h` là mặc định DAG (khai trong generator, một chỗ).
