# Index ADR — Architecture Decision Records

> Mỗi ADR ghi **một** quyết định kiến trúc: bối cảnh, quyết định, hệ quả, phương án đã cân nhắc. Nguyên tắc
> (ADR-0001): quyết định đáng kể thì ghi ADR, đứng tên tác giả, không sửa lịch sử — chỉ thêm ADR mới thay thế.
> Mẫu: [`template.md`](template.md). Trạng thái toàn cục: [roadmap](../roadmap/BDP-metadata-driven-roadmap.md) +
> [hiện trạng](../architecture/BDP-current-state.md).

## Nền tảng (Pha 0–1) — kiến trúc gốc

| ADR | Quyết định |
|---|---|
| [0001](0001-record-architecture-decisions.md) | Ghi quyết định dưới dạng ADR |
| [0002](0002-cdc-via-debezium-pgoutput.md) | CDC bằng Debezium + `pgoutput`, publication tường minh |
| [0003](0003-avro-with-schema-registry.md) | Avro + Schema Registry; `decimal.handling.mode=string` |
| [0004](0004-replica-identity-full-for-mutable-tables.md) | `REPLICA IDENTITY FULL` cho bảng mutable |
| [0005](0005-kafka-kraft-single-node.md) | Kafka KRaft single-node, RF=1 — phạm vi lab |
| [0006](0006-one-flink-job-per-lane-statement-set.md) | Một job Flink / lane bằng `StatementSet` |
| [0007](0007-clickhouse-kafka-engine-serving.md) | ClickHouse serving qua Kafka engine + MV |
| [0008](0008-medallion-on-minio-parquet.md) | Medallion Parquet trên MinIO |
| [0009](0009-iceberg-rest-catalog.md) | Iceberg qua REST catalog, `HadoopFileIO` với MinIO |
| [0010](0010-trino-for-federation.md) | Trino làm lớp truy vấn liên nguồn |
| [0011](0011-es-sink-upsert-by-primary-key.md) | ES sink upsert theo PK |
| [0012](0012-dlq-processor-not-wired.md) | DLQ processor chưa nối — ghi lại thay vì xoá |
| [0013](0013-secrets-in-gitignored-env.md) | Secrets trong `.env` gitignore — giới hạn lab |

## Bước ngoặt metadata-driven (Pha 1–2)

| ADR | Quyết định |
|---|---|
| [0014](0014-adopt-metadata-driven-roadmap.md) | Chọn metadata-driven làm đích kiến trúc |
| [0015](0015-metadata-registry-yaml-first.md) | Registry là YAML trong Git; `check` gác cửa drift |
| [0016](0016-rename-platform-to-dataplatform.md) | Đặt tên control plane `dataplatform/` |
| [0017](0017-dlq-flow-observe-then-park.md) | Nối DLQ — quan sát trước, phát lại sau |

## Sinh artifact từ contract (Pha 2–5)

| ADR | Quyết định |
|---|---|
| [0018](0018-generate-debezium-and-publication.md) | Sinh Debezium connector + publication — diệt sprawl #2/#3 |
| [0019](0019-generate-clickhouse-metric-ddl.md) | Sinh DDL ClickHouse — diệt sprawl #8/#9 |
| [0020](0020-generate-kafka-topic-manifest.md) | Sinh bản kê topic Kafka; tắt auto-create |
| [0021](0021-connector-deployer-idempotent.md) | Deployer idempotent — metadata thành load-bearing |
| [0022](0022-reverse-verify-contract-vs-real-schema.md) | Kiểm chứng ngược contract vs schema thật |
| [0023](0023-flink-metric-runner-declarative.md) | Flink runner khai báo — diệt sprawl #6/#8 |
| [0024](0024-spark-medallion-runner-sql.md) | Spark medallion runner — transform bằng SQL (dbt) |

## Federation, Catalog & Lineage (Pha 6)

| ADR | Quyết định |
|---|---|
| [0025](0025-connection-registry-trino-catalog.md) | Connection registry + sinh Trino catalog; federation 3 nguồn |
| [0026](0026-lineage-catalog-from-metadata.md) | Sinh lineage graph + catalog từ metadata |
| [0027](0027-openmetadata-catalog.md) | OpenMetadata làm catalog UI — nạp từ metadata |
| [0028](0028-spark-column-lineage-sqlglot.md) | Lineage cột Spark — parse SQL bằng sqlglot |
| [0029](0029-encode-connection-non-trino.md) | Encode connection non-Trino — registry là nguồn endpoint |

## Orchestration, CI/CD & Governance (Pha 7)

| ADR | Quyết định |
|---|---|
| [0030](0030-ci-plan-compat-gate.md) | CI plan → compat gate (BACKWARD) — GitOps cho contract |
| [0031](0031-airflow-dag-from-metadata.md) | Airflow DAG sinh từ phụ thuộc batch spec |
| [0032](0032-versioned-migration-clickhouse.md) | Migration có version cho ClickHouse — bỏ init-once |
| [0033](0033-data-quality-gate.md) | Data quality gate — luật chạy trên dữ liệu thật |
| [0034](0034-rollback-via-git-ref.md) | Rollback deployer — áp lại desired state từ git ref |
| [0035](0035-rbac-codeowners.md) | RBAC & audit — CODEOWNERS + owner contract |
| [0036](0036-iceberg-native-evolution.md) | Iceberg dùng schema evolution native (không runner) |

## Cắt chuyển & vận hành hoá (Pha 8)

| ADR | Quyết định |
|---|---|
| [0037](0037-cutover-complete-single-source.md) | Chốt cutover — `metadata/` là nguồn sự thật duy nhất |
| [0038](0038-om-governance-from-metadata.md) | Governance OM sinh từ metadata — domain/tier, test case, metric, dashboard, KPI |
