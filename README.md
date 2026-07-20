# Fintech Real-time CDC & Lakehouse Platform — Metadata-Driven

> Một nền tảng dữ liệu ngân hàng gần thời gian thực (CDC → streaming → lakehouse → federation), **vận
> hành hoàn toàn bằng metadata**: mọi connector, DDL, topic, catalog, DAG được **SINH TỰ ĐỘNG** từ một
> registry contract duy nhất trong Git, có cổng CI gác drift/tương thích và bộ verifier đối chiếu với hệ
> thống thật.

<p align="center">
  <img alt="Metadata-Driven" src="https://img.shields.io/badge/Metadata--Driven-single%20source%20of%20truth-6f42c1" />
  <img alt="CDC" src="https://img.shields.io/badge/CDC-Debezium%20%2F%20Kafka-black" />
  <img alt="Streaming" src="https://img.shields.io/badge/Streaming-Flink-orange" />
  <img alt="Lakehouse" src="https://img.shields.io/badge/Lakehouse-Iceberg%20%2B%20Spark%20%2B%20Trino-green" />
  <img alt="Governance" src="https://img.shields.io/badge/Catalog-OpenMetadata%20%2B%20Lineage-blue" />
  <img alt="ADRs" src="https://img.shields.io/badge/ADRs-37-informational" />
  <img alt="Drift gate" src="https://img.shields.io/badge/cli%20check-19%2F19%20byte--exact-success" />
</p>

<p align="center">
  <img src="assets/images/data_flow.png" alt="Kiến trúc tổng quan" width="90%" />
</p>

---

## Điểm nhấn kỹ thuật

Phần lõi của dự án **không phải** các đường ống dữ liệu (đó là phần "chạy được"), mà là **control plane
biến metadata thành nguồn sự thật duy nhất** — giải quyết "metadata sprawl": trước đây một "sự thật về bảng"
(cột gì, khóa gì, vào topic nào) bị chép tay ở ~10 nơi, đổi một cột phải sửa nhiều file và dễ sót.

| Trước (thủ công) | Sau (metadata-driven) |
|---|---|
| Sự thật schema rải rác ~10 nơi | **1 registry** `metadata/` (dataset + connection + pipeline + quality) |
| Thêm cột = sửa tối đa 6 file Flink + 3 ClickHouse + Spark + ES | Thêm cột = sửa **1 contract** → sinh lại tự động |
| Connector/DDL/topic viết tay, dễ lệch | **19 artifact SINH** từ contract, `cli check` **19/19 byte-exact** |
| Không gác thay đổi | CI: **drift gate** + **compat BACKWARD gate** + **plan** hệ quả artifact |
| Không lineage/catalog | Lineage cấp **cột** (Flink + Spark) + **OpenMetadata**, sinh từ metadata |
| Deploy thủ công qua REST/`curl` | **Deployer idempotent** (plan/apply) + **rollback** từ git ref |

> Toàn bộ hành trình được ghi trong **37 ADR** ([`docs/decisions/`](docs/decisions/README.md)) và một
> [roadmap 8 pha](docs/roadmap/BDP-metadata-driven-roadmap.md) đã **hoàn tất & kiểm chứng live**.

---

## Control plane — metadata sinh ra mọi thứ

Đây là tầng cốt lõi. `metadata/` là đầu vào duy nhất; **generators** sinh artifact, `cli check` gác
**byte-exact**, **deployers** áp lên runtime, **verifiers** đối chiếu ngược với hệ thống thật:

```mermaid
flowchart LR
    subgraph SOT["metadata/ — NGUỒN SỰ THẬT DUY NHẤT"]
        direction TB
        DS["datasets<br/>(oltp · metrics · alerts)"]
        CN["connections"]
        PP["pipelines<br/>(stream · batch)"]
        QL["quality"]
    end
    SOT --> GEN["generators (11)<br/>contract → artifact"]
    GEN --> ART["19 artifact SINH<br/>connector JSON · DDL · topic<br/>Trino catalog · Airflow DAG · lineage"]
    ART --> CHK{"cli check<br/>byte-exact?"}
    CHK -->|"khớp 19/19"| DEP["deployers (5)<br/>plan · apply · rollback"]
    CHK -.->|"lệch → CI ĐỎ"| SOT
    DEP --> RT["RUNTIME<br/>Kafka Connect · ClickHouse<br/>Flink · Spark · Trino · OpenMetadata"]
    RT --> VER["verifiers (4)<br/>đối chiếu contract vs hệ thống THẬT"]
    VER -.->|"phát hiện drift"| SOT
```

Mọi thay đổi đi qua **một vòng lặp 6 bước** (chiến lược *strangler-fig* — bóp nghẹt dần bản viết tay):

```mermaid
flowchart LR
    A["① sửa<br/>contract"] --> B["② cli write<br/>(sinh)"]
    B --> C["③ cli check<br/>byte-exact vs bản cũ"]
    C --> D["④ deployer apply<br/>(cắt chuyển)"]
    D --> E["⑤ xóa<br/>bản viết tay"]
    E --> F["⑥ verify<br/>+ ADR"]
    F --> A
```

> Bước ③ chứng minh bản sinh == bản cũ **trước khi** dám thay → cắt chuyển không rủi ro. Chi tiết trình tự:
> [`METADATA-DRIVEN-cac-buoc-trien-khai.md`](METADATA-DRIVEN-cac-buoc-trien-khai.md).

**Ba lớp trong `dataplatform/`:**

| Lớp | Số | Vai trò | Ví dụ |
|---|---|---|---|
| **generators** | 11 | Contract → artifact | debezium · clickhouse_ddl · es_sink · s3_sink · flink_sql · topic_manifest · trino_catalog · lineage · airflow_dag · postgres_publication · dlq |
| **deployers** | 5 | Áp desired state (idempotent, plan/apply, rollback) | connectors · clickhouse_migrate · spark_batch · flink_metrics · openmetadata |
| **verifiers** | 4 | Đối chiếu contract vs hệ thống THẬT | postgres_schema · clickhouse_schema · avro_schema · quality |

---

## Runtime — luồng dữ liệu

```mermaid
flowchart LR
    PG[("PostgreSQL<br/>OLTP")] -->|"Debezium CDC (WAL→Avro)"| K[("Kafka")]
    K --> FM["Flink<br/>metrics"] --> CH[("ClickHouse")] --> GR["Grafana"]
    K --> FR["Flink<br/>fraud"] --> FA["fraud-alerts"]
    K --> ESK["ES sink"] --> ES[("Elasticsearch")] --> KB["Kibana"]
    FA --> ES
    K --> S3["S3 sink"] --> MO[("MinIO<br/>Bronze")]
    MO --> SP["Spark<br/>medallion"] --> LK[("Silver / Gold<br/>+ Iceberg")]
    PG --> TR["Trino<br/>federation"]
    CH --> TR
    LK --> TR
    OM["OpenMetadata<br/>catalog + lineage cột<br/>(sinh từ metadata)"]
```

<table>
<tr>
<td width="50%"><b>CDC nguồn (Debezium)</b><br/><img src="assets/images/postgres_source.png" /></td>
<td width="50%"><b>Kafka topics (CDC · metrics · fraud)</b><br/><img src="assets/images/kafka.png" /></td>
</tr>
<tr>
<td><b>Flink jobs (streaming)</b><br/><img src="assets/images/flink.png" /></td>
<td><b>Grafana realtime dashboard</b><br/><img src="assets/images/dashboard.png" /></td>
</tr>
<tr>
<td><b>MinIO lakehouse (Bronze/Silver/Gold/Iceberg)</b><br/><img src="assets/images/MinIO.png" /></td>
<td><b>Kibana — điều tra fraud/failed</b><br/><img src="assets/images/dashboard_els.png" /></td>
</tr>
</table>

---

## Năng lực đã xây & kiểm chứng (Pha 1–8, hoàn tất)

| Pha | Năng lực | Kiểm chứng live |
|---|---|---|
| 1–2 | Contract registry + sinh ingestion (Debezium, publication, topic, ClickHouse DDL) + deployer idempotent + CI drift gate | `check` 19/19 · `avro_schema` 0 lệch · auto-create.topics tắt |
| 3 | Flink runner khai báo (metric + fraud) sinh từ pipeline spec | job chạy · DDL khớp ClickHouse |
| 4 | ClickHouse serving sinh từ contract | `clickhouse_schema` 0 drift |
| 5 | Spark medallion (Silver/Gold/Iceberg) SQL-in-spec, chạy theo phụ thuộc | Iceberg 1.072 rows |
| 6 | Trino federation + lineage cấp cột (sqlglot) + OpenMetadata catalog | query 3 nguồn · OM 24 table, 25 cạnh |
| 7 | CI plan/compat gate · Airflow DAG sinh từ deps · migration versioned · **data quality gate** · rollback · RBAC | quality 66 check · DAG load OK · migration idempotent |
| 8 | Cutover chốt + runbook — "một nơi để sửa: `metadata/`" | không còn file viết tay song song |

*Ngoài phạm vi metadata-driven (trục riêng, chưa làm):* bảo mật (secret manager + auth service),
HA/robustness, Silver incremental.

---

## Cấu trúc thư mục

```text
bigdata-platform/
├── metadata/                    # ★ NGUỒN SỰ THẬT DUY NHẤT (contract YAML)
│   ├── datasets/                #   oltp / metrics / alerts
│   ├── connections/             #   postgres, clickhouse, iceberg, kafka, es, s3, schema-registry
│   ├── pipelines/               #   stream (Flink) / batch (Spark)
│   └── quality/                 #   luật data quality
├── dataplatform/                # ★ CONTROL PLANE (Python)
│   ├── registry.py  cli.py  compat.py
│   ├── schemas/                 #   JSON Schema validate contract
│   ├── generators/  deployers/  verifiers/
├── migrations/                  # versioned migration (clickhouse/ + iceberg native)
├── lineage/                     # graph.json + LINEAGE.md (sinh)
├── airflow/  openmetadata/      # runtime phiên-riêng (compose riêng + DAG sinh)
├── debezium/ kafka-connect/ kafka/ clickhouse/ trino/ postgres/   # ARTIFACT SINH (đừng sửa tay)
├── flink/ spark/ dlq-processor/ fraud-notifier/ generator/        # runtime services + runner generic
├── docs/                        # decisions/ (37 ADR) · roadmap/ · architecture/ · guide/ (runbook)
├── .github/                     # CI (metadata-check) + CODEOWNERS
└── docker-compose.yml
```

> Các thư mục `debezium/`, `kafka-connect/`, `clickhouse/init/`, `kafka/`, `trino/etc/catalog/` là
> **artifact sinh** — không sửa tay (CI `check` sẽ đỏ); sửa contract rồi `cli write`.

---

## Tech stack

| Layer | Công nghệ | Vai trò |
|---|---|---|
| Control plane | Python 3.12, PyYAML, jsonschema, sqlglot | Sinh/gác/áp/đối chiếu từ metadata |
| Source DB | PostgreSQL 16 | OLTP nguồn, logical replication |
| CDC | Debezium | WAL → Avro CDC event |
| Backbone | Kafka (KRaft) + Schema Registry | Truyền sự kiện + Avro schema |
| Streaming | Apache Flink 1.18 (PyFlink) | Realtime metrics + fraud detection |
| Serving OLAP | ClickHouse (Kafka Engine + MV) + Grafana | Metrics tốc độ cao + dashboard |
| Search | Elasticsearch + Kibana | Tra cứu/điều tra CDC + fraud alert |
| Lakehouse | MinIO (S3) + Spark 3.5 + Apache Iceberg (REST catalog) | Bronze/Silver/Gold + snapshot/time-travel |
| Federation | Trino | Query chéo Postgres × ClickHouse × Iceberg |
| Catalog/Lineage | OpenMetadata | Discovery + lineage cấp cột + PII tag |
| Orchestration | Apache Airflow (DAG sinh từ deps) | Lịch batch medallion |
| Runtime | Docker Compose | Chạy toàn bộ local/dev |

---

## Quick start

**1) Control plane** (không cần Docker — thuần tĩnh):

```bash
pip install -r requirements-dev.txt
python -m dataplatform.cli check      # 19/19 — artifact khớp metadata
python -m dataplatform.cli write      # sinh lại toàn bộ artifact từ metadata/
python -m dataplatform.cli plan       # (trên PR) hệ quả artifact khi merge
python -m dataplatform.cli compat     # (trên PR) gate BACKWARD
```

**2) Runtime platform:**

```bash
cp .env.example .env                  # điền secret (không commit)
docker compose up -d                  # dựng toàn bộ stack
docker compose up -d kafka-init       # tạo topic (auto.create.topics=false)
```

<p align="center">
  <img src="assets/images/start.png" alt="Các service đã khởi động" width="80%" />
</p>

**3) Áp cấu hình từ metadata** (thay cho đăng ký connector thủ công):

```bash
python -m dataplatform.deployers.connectors        apply   # Debezium + ES/S3 sink
python -m dataplatform.deployers.clickhouse_migrate apply   # schema + migration
python -m dataplatform.deployers.flink_metrics     apply   # Flink runner
python -m dataplatform.deployers.spark_batch       apply   # medallion Silver→Gold→Iceberg
```

**4) Đối chiếu với hệ thống thật:**

```bash
python -m dataplatform.verifiers.avro_schema        # Avro trên dây vs contract
python -m dataplatform.verifiers.clickhouse_schema  # bảng CH vs contract
python -m dataplatform.verifiers.quality            # data quality gate
```

> Catalog UI (OpenMetadata) và orchestration (Airflow) chạy **phiên-riêng** (compose riêng, RAM) — xem
> [`docs/guide/runbook.md`](docs/guide/runbook.md).

**Service URLs:** Kafka UI `:8080` · Connect `:8083` · Schema Registry `:8081` · Flink `:8082` ·
ClickHouse `:8123` · Grafana `:3000` · MinIO `:9001` · Kibana `:5601` · Trino `:8085` ·
OpenMetadata `:8585` · Airflow `:8090`

---

## Reliability & Observability

Mọi sink bật **dead-letter queue**; DLQ processor phân loại lỗi transient/permanent/unknown thành dữ liệu
truy vấn được. Theo dõi qua Kafka UI · Flink UI · Spark UI · Grafana · Kibana · MinIO Console.

<table>
<tr>
<td width="50%"><b>DLQ processor — phân loại lỗi</b><br/><img src="assets/images/DLQ.png" /></td>
<td width="50%"><b>Fault handling / observability</b><br/><img src="assets/images/fault.png" /></td>
</tr>
</table>

---

## Thực hành kỹ thuật

- **ADR-first:** mọi quyết định đáng kể có một ADR (bối cảnh · quyết định · hệ quả · phương án đã cân nhắc) —
  [37 ADR](docs/decisions/README.md).
- **Oracle byte-exact:** generator được chứng minh sinh ra *đúng từng byte* bản viết tay trước khi cắt chuyển.
- **CI gates** (`.github/workflows/metadata-check.yml`): drift (`check`) + BACKWARD (`compat`) + plan hệ quả, thuần tĩnh.
- **Verify runtime:** đối chiếu contract với schema THẬT (Postgres/ClickHouse/Avro-trên-dây).
- **RBAC/audit:** `.github/CODEOWNERS` theo vùng metadata + `owner` trong contract + audit Git/lineage.

---

## Tài liệu

| Muốn hiểu | Đọc |
|---|---|
| Trình tự triển khai (làm từ đâu tới đâu) | [`METADATA-DRIVEN-cac-buoc-trien-khai.md`](METADATA-DRIVEN-cac-buoc-trien-khai.md) |
| Cái đích + từng pha | [`docs/roadmap/BDP-metadata-driven-roadmap.md`](docs/roadmap/BDP-metadata-driven-roadmap.md) |
| Điểm xuất phát (metadata sprawl) | [`docs/architecture/BDP-current-state.md`](docs/architecture/BDP-current-state.md) |
| Vận hành hằng ngày + gotchas | [`docs/guide/runbook.md`](docs/guide/runbook.md) |
| Vì sao mỗi quyết định | [`docs/decisions/README.md`](docs/decisions/README.md) (index 37 ADR) |

---

## Tác giả

**Phan Văn Trường** — Data Engineering · Fintech CDC, Streaming & Metadata-Driven Lakehouse Platform
