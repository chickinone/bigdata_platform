# Runbook — vận hành nền tảng metadata-driven

> **Một nơi để sửa: `metadata/`.** Mọi thay đổi dữ liệu/hạ tầng đều quy về: sửa contract → sinh lại
> artifact → gate → áp. Runbook này liệt kê từng tác vụ thường gặp = "sửa gì + chạy gì + verify gì".
> Nền tảng: xem [hiện trạng](../architecture/BDP-current-state.md), [roadmap](../roadmap/BDP-metadata-driven-roadmap.md),
> [index ADR](../decisions/README.md).

## Vòng lặp chuẩn (mọi thay đổi)

```
1. Sửa metadata/           (dataset / connection / pipeline / quality)
2. python -m dataplatform.cli write     # sinh lại artifact
3. python -m dataplatform.cli check     # 19/19 — artifact khớp metadata (không thì sửa tiếp)
4. git commit + mở PR
     CI tự chạy: check (drift) + compat (BACKWARD) + plan (hệ quả artifact)
5. merge → deployer apply (idempotent)  # connectors / clickhouse_migrate / spark_batch / flink_metrics / openmetadata
6. verify runtime           # verifiers/* + quality gate
```

**Không bao giờ sửa tay file sinh** (connector JSON, DDL, catalog, DAG, lineage) — CI `check` sẽ đỏ. Sửa
contract rồi `write`.

---

## Tác vụ thường gặp

### Thêm một cột vào dataset

1. Sửa `metadata/datasets/<layer>/<name>.yaml` — thêm vào `columns`. Cột mới nên `nullable: true` (an toàn
   BACKWARD; xem [ADR-0030](../decisions/0030-ci-plan-compat-gate.md)).
2. `cli write && cli check`.
3. Nếu cột vào metric ClickHouse **đang sống**: thêm migration `migrations/clickhouse/000N_add_<col>.sql`
   (`ALTER TABLE ... ADD COLUMN`) — `IF NOT EXISTS` của init không đụng bảng cũ ([ADR-0032](../decisions/0032-versioned-migration-clickhouse.md)).
   Rồi `python -m dataplatform.deployers.clickhouse_migrate apply`.
4. Verify: `python -m dataplatform.verifiers.clickhouse_schema` (live khớp contract).

### Thêm một dataset / bảng mới

1. Tạo `metadata/datasets/<layer>/<name>.yaml` (theo mẫu dataset cùng layer). Khai `source`, `columns`,
   `primary_key`, `sinks`.
2. `cli write` → tự sinh: Debezium `table.include.list`, publication, ES/S3 sink, topic, DDL ClickHouse,
   lineage, DAG. `cli check`.
3. Áp: `connectors apply` (Kafka Connect), `clickhouse_migrate apply` nếu có sink CH.
4. Bảng metric mới = init sinh tự có (idempotent), không cần migration.

### Thêm một metric (Flink → ClickHouse)

1. `metadata/pipelines/stream/<metric>.yaml` (spec Flink) + `metadata/datasets/metrics/<metric>.yaml`
   (sink ClickHouse). Cột metric khai một chỗ → sinh ROW Flink + bảng đích + Kafka + MV khớp nhau ([ADR-0023](../decisions/0023-flink-metric-runner-declarative.md)/[0019](../decisions/0019-generate-clickhouse-metric-ddl.md)).
2. `cli write && cli check`.
3. Áp: `flink_metrics apply` (resubmit runner) + `clickhouse_migrate apply`.

### Thêm một connection (nguồn/đích mới)

1. `metadata/connections/<name>.yaml`: `name`, `type`, `endpoints` (placeholder `${env:...}`), và khối
   `trino` nếu Trino query được ([ADR-0025](../decisions/0025-connection-registry-trino-catalog.md)/[0029](../decisions/0029-encode-connection-non-trino.md)).
2. `cli write` → sinh Trino catalog (nếu có `trino`); generator khác đọc `endpoints`. `cli check`.

### Xử lý breaking change (compat gate chặn)

Nếu PR đổi type không promote được / thêm cột `nullable:false` / biến optional→required → CI `compat` đỏ
([ADR-0030](../decisions/0030-ci-plan-compat-gate.md)). Cách xử:
- **Ưu tiên:** đổi thành additive (cột mới nullable, giữ cột cũ) — tương thích ngược.
- Nếu buộc phá: bump version dataset/topic (contract mới song song), migrate consumer, rồi bỏ cũ. Không
  ép merge qua gate.
- Chạy tại chỗ: `python -m dataplatform.cli compat --base origin/main`.

### Migration ClickHouse (thay đổi bảng đang sống)

```bash
# 1. Thêm file bất biến, số tăng dần:
#    migrations/clickhouse/000N_<mo_ta>.sql   (ALTER TABLE ... / CREATE TABLE ...)
python -m dataplatform.deployers.clickhouse_migrate plan    # xem chờ áp
python -m dataplatform.deployers.clickhouse_migrate apply   # áp (idempotent, ghi schema_migrations)
```
**Đã áp thì đừng sửa file** (checksum guard sẽ báo lỗi) — sai thì thêm migration mới (forward-only).
Iceberg: dùng `ALTER TABLE` native + snapshot, không runner ([ADR-0036](../decisions/0036-iceberg-native-evolution.md), `migrations/iceberg/README`).

### Rollback (quay lui khi đổi hỏng)

```bash
python -m dataplatform.deployers.connectors plan  --ref <commit-tốt>   # xem rollback đổi gì
python -m dataplatform.deployers.connectors apply --ref <commit-tốt>   # áp lại config connector ở ref đó
```
Áp lại desired state đã commit ở ref cũ ([ADR-0034](../decisions/0034-rollback-via-git-ref.md)). Rollback dữ
liệu Iceberg: `CALL iceberg.system.rollback_to_snapshot(...)`.

### Data quality (kiểm dữ liệu thật)

- not_null (cột `nullable:false`) + unique (`primary_key`) **tự suy** từ contract — không khai lại.
- Luật tường minh: `metadata/quality/<dataset>.yaml` (`range`, `accepted_values`).
- Chạy gate: `python -m dataplatform.verifiers.quality` (fail → chặn promote, [ADR-0033](../decisions/0033-data-quality-gate.md)).

### Backfill

- Batch: `python -m dataplatform.deployers.spark_batch apply` (chạy lại silver→gold→iceberg theo thứ tự
  phụ thuộc). Silver hiện là full-refresh overwrite (nợ #13).
- Qua Airflow: trigger DAG `medallion_batch` (cần stack Spark sống).

---

## Runtime phiên-riêng (máy 15GB, không chạy tất cả cùng lúc)

| Việc | Bật gì | Cổng |
|---|---|---|
| Catalog UI | `docker compose -f openmetadata/docker-compose-openmetadata.yml up -d openmetadata-server elasticsearch` | 8585 |
| Federation query | `docker compose up -d minio iceberg-rest trino` (+ postgres/clickhouse) | trino 8085 |
| Orchestration | `docker compose -f airflow/docker-compose-airflow.yml up -d` (+ stack Spark cho task) | 8090 |

Dừng bớt để nhường RAM: `docker compose stop` (stack chính) / `... -f <file> stop`.

---

## Gotchas (đã trả bằng thời gian debug — đừng vấp lại)

| Triệu chứng | Nguyên nhân | Xử |
|---|---|---|
| PUT lineage OM trả 500 `[elasticsearch]` | ES container OOM-kill (exit 137) | Bật lại ES, đợi `yellow`: `curl localhost:9200/_cluster/health` |
| Iceberg query "biến mất"/lỗi table | `tabulario/iceberg-rest` lưu catalog trong RAM, restart mất bảng | Chạy lại Spark iceberg job đăng ký lại trước khi query |
| `clickhouse_migrate`/quality lỗi encode | `subprocess input` mặc định cp1252 (Windows) | Đã fix `encoding="utf-8"`; comment tiếng Việt cần UTF-8 |
| `docker exec /opt/...` → `C:/Program Files/Git/opt/...` | Git Bash mangle path Unix | Prefix `MSYS_NO_PATHCONV=1` |
| Migration runner treo | Bảng `ENGINE=Kafka` cần broker; Kafka down | Runner chỉ áp `migrations/`, không áp baseline init (cần Kafka) |
| Airflow DAG không load, dags rỗng | Volume `./airflow/dags` sai (project-dir là `airflow/`) | Dùng `./dags` trong compose airflow |
| CDC không produce, log `UNKNOWN_TOPIC_OR_PARTITION` | `auto.create.topics=false` (ADR-0020) + chưa tạo topic | `docker compose up -d kafka-init` (chạy create-topics.sh) trước khi CDC produce |
| Sau restart chỉ vài bảng có Avro schema | Slot Debezium bền (PG volume) nhưng Schema Registry reset → resume từ offset cũ, không re-snapshot | Xoá connector → đợi slot `active=f` → `pg_drop_replication_slot` → re-apply (fresh snapshot). Bảng rỗng thì không có schema — đúng, không phải lỗi |
| OM search trả 0 table (entity vẫn còn) | ES của OM chết → search rỗng dù postgres còn entity | Bật lại ES; hoặc nạp lại `openmetadata apply` (catalog tái tạo từ `graph.json`) |
| File cứ hiện "modified" (LF↔CRLF) | Generator ghi LF, Git chuẩn hoá CRLF | Nhiễu vô hại; `git checkout -- <file>` nếu không có diff thật |

---

## Chốt

Hệ thống nay chỉ còn **một nơi để sửa — `metadata/`**. Thêm cột/bảng/metric/connection = sửa YAML + chạy
generator + deployer; CI gác drift + BACKWARD; quality + verifier gác dữ liệu; rollback + migration + DAG
đều suy từ metadata. Không còn "sự thật về dữ liệu" nào bị chép tay rải rác (metadata sprawl — xem
[hiện trạng §3](../architecture/BDP-current-state.md)).
