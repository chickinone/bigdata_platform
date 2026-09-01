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
5. merge → python -m dataplatform.cli apply    # soạn 4 deployer đúng thứ tự + gate giữa các bước
6. cli apply tự chạy `cli verify` ở cuối       # 7 verifier, một mã thoát
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
4. **Bảng CDC trên DB đang sống:** `04_publication.sql` là init script, không chạy lại → phải tự gõ
   `ALTER PUBLICATION dbz_publication ADD TABLE <schema>.<table>;`. Quên bước này thì bảng có snapshot
   nhưng **không bao giờ có bản ghi mới**, connector vẫn `RUNNING` và `cli check` vẫn xanh.
   Kiểm bằng `python -m dataplatform.verifiers.postgres_publication` ([ADR-0039](../decisions/0039-verify-publication-vs-contract.md)).
5. **Topic trên cluster đang sống:** `create-topics.sh` chỉ chạy khi dựng stack (`kafka-init`), nên chạy tay
   `docker exec bigdata-kafka bash /opt/bitnami/kafka/create-topics.sh` (idempotent). Kiểm bằng
   `python -m dataplatform.verifiers.kafka_topics` ([ADR-0040](../decisions/0040-tighten-topic-layer.md)).
6. Bảng metric mới = init sinh tự có (idempotent), không cần migration.

**Cần nhiều partition / retention riêng cho một topic?** Khai `source.partitions` / `source.topic_configs`
trong contract dataset, không sửa generator ([ADR-0040](../decisions/0040-tighten-topic-layer.md)). Lưu ý:
đổi partition của topic nguồn làm thay đổi phân bố `key_by` của fraud detector — thay đổi có chủ ý, cần đối
chiếu lại, và `--if-not-exists` **không** sửa được topic đã tồn tại (phải xoá/tạo lại hoặc dùng
`kafka-topics --alter`).

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

### Backfill / chạy lại batch

Batch chạy theo **cửa sổ ngày** ([ADR-0042](../decisions/0042-incremental-batch-and-blast-radius.md)):
mỗi lần chạy tính lại trọn vẹn `lookback_days` ngày gần nhất và ghi đè đúng những partition đó.
Chạy lại cùng `--as-of` là idempotent.

| Muốn gì | Lệnh |
|---|---|
| Chạy cửa sổ hôm nay | `python -m dataplatform.deployers.spark_batch apply` |
| Vá một ngày cũ | `... apply --as-of 2026-08-01` (tính lại cửa sổ kết thúc ở ngày đó) |
| Tính lại toàn bộ lịch sử | `... apply --full-refresh` |
| Qua Airflow | trigger/clear task DAG `medallion_batch` — `AS_OF` lấy từ `{{ds}}`, nên clear một ngày cũ tự backfill đúng ngày đó |

**Khi nào cần `--full-refresh`:**

- Sau khi sửa chiều (đổi tên khách hàng, đổi `risk_score`): cửa sổ chỉ cập nhật dòng trong cửa sổ, dòng cũ
  giữ giá trị tại thời điểm giao dịch. Chạy full refresh để đồng bộ lại toàn bộ.
- Dữ liệu về muộn hơn `lookback_days` (mặc định 3 ngày).
- Nên đặt lịch định kỳ (tuần/tháng) chứ đừng đợi phát hiện lệch.

**Đọc log để tự kiểm:** runner in `cửa sổ <start> .. <end>` và `thay N partition: ...`. Chạy lại cùng
`--as-of` phải ra **đúng cùng danh sách partition** — khác là có vấn đề.

**Kiểm sâu hơn (đếm dòng từng partition):**

```bash
MSYS_NO_PATHCONV=1 docker exec bigdata-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/partition_census.py s3a://data-lake-silver/enriched_transactions/
```

Quy trình chứng minh: `apply --full-refresh` → census (mốc chuẩn) → `apply` → census → so. Tổng và từng
partition phải khớp. Đây là cách ADR-0042 được verify.

> `gold_customer_lifetime_metrics` và `iceberg_silver_enriched` cố ý vẫn full refresh mỗi lần chạy — gộp
> trọn đời không khoanh theo ngày được, còn Iceberg đã ghi nguyên tử sẵn. Lý do ghi trong chính hai spec.

### Cập nhật governance catalog (domain / tier / test case / dashboard)

Mọi thứ hiển thị trong OpenMetadata đều suy từ metadata ([ADR-0038](../decisions/0038-om-governance-from-metadata.md)):

| Muốn đổi | Sửa ở đâu |
|---|---|
| Domain / tier / owner / mô tả của dataset | `metadata/datasets/<layer>/<name>.yaml` (`domain:`, `tier:`, `owner:`, `description:`) |
| Test case data quality | `metadata/quality/<dataset>.yaml` (range/accepted_values) — not_null/unique tự suy từ contract. Kết quả chạy thật: `verifiers.quality --push-om` (cần Postgres/ClickHouse sống) |
| Metric definition | `metadata/pipelines/stream/<metric>.yaml` (aggregations) |
| Dashboard Grafana + lineage | `metadata/connections/grafana.yaml` (khối `dashboards`) |

Rồi: `cli write && cli check` (nếu đổi dataset) → bật OM (phiên riêng) →
`python -m dataplatform.deployers.openmetadata apply` (idempotent — replace nguyên khối, chạy lại không nhân đôi).
Không gõ governance trên UI — lần `apply` sau sẽ ghi đè theo contract.

---

## Runtime phiên-riêng (máy 15GB, không chạy tất cả cùng lúc)

| Việc | Bật gì | Cổng |
|---|---|---|
| Catalog UI | `docker compose -f openmetadata/docker-compose-openmetadata.yml up -d openmetadata-server elasticsearch` | 8585 |
| Federation query | `docker compose up -d minio iceberg-rest trino` (+ postgres/clickhouse) | trino 8085 |
| Orchestration | `docker compose -f airflow/docker-compose-airflow.yml up -d` (+ stack Spark cho task) | 8090 |
| BI (Superset) | `docker compose -f superset/docker-compose-superset.yml up -d` (+ clickhouse/postgres tuỳ dashboard) | 8088 |

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
| OM search hiện entity "ma" của project khác (đã xoá khỏi DB) | Search index (ES) lệch DB — sự kiện xoá không tới ES (ES down/OOM lúc xoá). OM instance này từng phục vụ project khác trên cùng volume | Trigger reindex: `POST /api/v1/apps/trigger/SearchIndexingApplication` (app có `recreateIndex: true`) rồi đợi status `success` |
| File cứ hiện "modified" (LF↔CRLF) | Generator ghi LF, Git chuẩn hoá CRLF | Nhiễu vô hại; `git checkout -- <file>` nếu không có diff thật |
| Postgres ngừng nhận write, `pg_wal` phình to | Slot Debezium kẹt (Kafka/connector chết) níu WAL — sự cố analytics giết DB nguồn | Đã chặn: `max_slot_wal_keep_size=2GB` (ADR-0042). Slot bị invalidate thì DB sống nhưng **mất liên tục CDC** → xoá connector, `pg_drop_replication_slot`, re-apply để snapshot lại |
| Slot `wal_status = lost` trong `pg_replication_slots` | Đã vượt trần WAL, slot bị invalidate có chủ đích | Đúng thiết kế (thà mất CDC hơn mất DB). Re-snapshot như dòng trên |
| Batch job chạy xong nhưng Silver/Gold thiếu ngày cũ | Cửa sổ `lookback_days` chỉ tính lại vài ngày gần nhất — đúng thiết kế | `apply --as-of <ngày>` để vá ngày đó, hoặc `--full-refresh` |
| Task Airflow xanh mà không có dữ liệu | ~~BashOperator chỉ xét exit code~~ đã vá (ADR-0042): `grep '^WROTE '` là trọng tài | Nếu vẫn gặp: xem `/tmp/medallion_<job>.*.log` trong container Airflow |
| Trino `unhealthy` mãi nhưng query vẫn chạy | File `.properties` có CRLF → script `health-check` (bash) dựng URL `localhost:8080\r` → `curl: (3)`. Java trim được nên Trino vẫn sống | Đã chặn: `*.properties text eol=lf` ([ADR-0043](../decisions/0043-cold-rebuild-findings.md)). Kiểm: `tr -cd '\r' < trino/etc/config.properties \| wc -c` phải ra 0 |
| ClickHouse 0 bảng sau khi dựng lạnh | compose KHÔNG mount `clickhouse/init/` — baseline DDL không tự chạy | Nạp tay: `for f in clickhouse/init/*.sql; do docker exec -i bigdata-clickhouse clickhouse-client --multiquery < $f; done` rồi `clickhouse_migrate apply` |
| `cli check` báo khớp mà file vẫn sai | `read_text()` bật universal newlines → CRLF bị chuẩn hoá TRƯỚC khi so, nên check mù về nó | Chưa vá (ADR-0043 việc #1). Kiểm CRLF bằng `tr -cd '\r'`, đừng tin check cho việc này |
| Không biết Trino/ClickHouse có thật sự đúng không | Chỉ 4/19 container có healthcheck; không có verifier cho Trino | Chạy tay cả 6 verifier (xem mục dưới). Trino chưa có verifier — ADR-0043 việc #2 |
| Docker khởi động lại, 15 service về nhưng Spark/Trino/iceberg-rest nằm im | Bốn service từng thiếu `restart:` nên mặc định `no` | Đã sửa thành `unless-stopped` ([ADR-0044](../decisions/0044-cli-apply-orchestrator.md)). Kiểm policy bằng `docker inspect` |
| Không biết stack có thiếu container nào không | Chỉ 4/19 có healthcheck; `docker ps` chỉ nói tiến trình chưa chết | `cli apply` đi hết chuỗi và bắt buộc từng bước đậu — đây là thứ đã phát hiện Spark chết âm thầm 30 phút |

---

## Dựng lạnh từ số không (sau khi stack bị xoá / `prune`)

Volume mới nghĩa là **không bước nào được bỏ qua** — thứ tự dưới đây là bắt buộc, đã chạy thông 30/08.

```bash
docker compose up -d
```

Chờ đủ 19 container. Ba việc tự chạy: postgres init (4 bảng + publication), `kafka-init` (22 topic),
`minio-init` (6 bucket). Kiểm nhanh:

```bash
docker exec bigdata-kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic bankdb.public.transactions
```

Sinh dữ liệu rồi áp toàn bộ bằng **một lệnh** ([ADR-0044](../decisions/0044-cli-apply-orchestrator.md)):

```bash
docker compose --profile generator up -d generator
```

```bash
python -m dataplatform.cli apply
```

`cli apply` chạy 4 bước theo đúng thứ tự phụ thuộc, có **điều kiện tiên quyết** giữa các bước, rồi
chạy `cli verify` ở cuối:

| # | Bước | Điều kiện trước |
|---|---|---|
| 1 | `connectors apply` | — |
| 2 | `clickhouse_migrate baseline` rồi `apply` | — |
| 3 | `flink_metrics apply` | **`clickhouse_schema` phải ĐẠT** |
| 4 | `spark_batch apply` | — |

Bước 3 là chỗ đáng chú ý: Flink **không cần** bảng ClickHouse để *submit*, nó chỉ cần để *ghi*. Không
có gate thì job xanh mà dữ liệu không tới đâu — đúng lỗi đã gặp khi dựng lạnh ([ADR-0043](../decisions/0043-cold-rebuild-findings.md)).

Xem trước không đụng gì: `cli apply --dry-run`. Chạy đúng một bước: `cli apply --only clickhouse`.
Thêm OpenMetadata (phiên riêng): `--with-openmetadata`. Backfill: `--as-of` / `--full-refresh` được
chuyển thẳng cho `spark_batch`.

**Từng deployer vẫn gọi riêng được** — `cli apply` soạn chúng chứ không thay. Lúc sự cố cần sửa đúng
một chỗ thì dùng lệnh lẻ:

```bash
python -m dataplatform.deployers.connectors apply
```

**Chạy cả 7 verifier một lượt:**

```bash
python -m dataplatform.cli verify
```

Mã thoát phân biệt hai loại thất bại — đừng gộp chúng khi đặt cảnh báo:

| Mã | Nghĩa | Phải làm |
|---|---|---|
| 0 | mọi verifier đạt | — |
| 1 | engine sống nhưng **lệch** contract | sửa contract hoặc áp lại deployer |
| 3 | **không tới được** engine | bật stack; chưa kết luận được gì |

**Chạy tự động mỗi giờ** — `scripts/verify-scheduled.cmd` qua Windows Task Scheduler ([ADR-0043](../decisions/0043-cold-rebuild-findings.md)).
Kết quả ở `.verify/history.log` (một dòng mỗi lần chạy) và `.verify/last-run.txt` (output đầy đủ lần gần nhất).

```bash
schtasks /Query /TN "BDP-verify" /FO LIST
```

Đăng ký lại trên máy khác (script nằm trong repo, chỉ lịch là cục bộ):

```bash
schtasks /Create /TN "BDP-verify" /TR "D:igdata-platform\scriptserify-scheduled.cmd" /SC HOURLY /F
```

Lưu ý: **`schtasks` cần `MSYS_NO_PATHCONV=1` trên Git Bash**, nếu không `/Query` bị dịch thành đường dẫn.

**Chốt cuối — federation 3 nguồn:**

```bash
docker exec bigdata-trino trino --execute "SELECT 'pg' n, count(*) c FROM postgres.public.transactions UNION ALL SELECT 'ice', count(*) FROM iceberg.silver.enriched_transactions UNION ALL SELECT 'ch', count(*) FROM clickhouse.metrics.timeseries"
```

**Lưu ý:** image `bigdata-pyflink:1.18.1` **build tại chỗ**, bước `pip install apache-flink` mất ~20
phút. Đừng `docker system prune -a` nếu không thật sự cần — `docker compose stop` hoặc `down` (KHÔNG
`-v`) giữ được cả volume lẫn image, dựng lại trong khoảng một phút.

---

## Chốt

Hệ thống nay chỉ còn **một nơi để sửa — `metadata/`**. Thêm cột/bảng/metric/connection = sửa YAML + chạy
generator + deployer; CI gác drift + BACKWARD; quality + verifier gác dữ liệu; rollback + migration + DAG
đều suy từ metadata. Không còn "sự thật về dữ liệu" nào bị chép tay rải rác (metadata sprawl — xem
[hiện trạng §3](../architecture/BDP-current-state.md)).
