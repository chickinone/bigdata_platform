# Bigdata Platform — chỉ mục tài liệu

Toàn bộ tài liệu dự án nằm ở đây, chia theo nhóm. File này là **bản đồ** — bắt đầu từ đây rồi mở
tài liệu bạn cần. Cập nhật lần cuối: 2026-07-15.

**Người mới? Đọc theo thứ tự này:** [`../README.md`](../README.md) (dự án là gì) →
[`architecture/BDP-architecture-overview.md`](architecture/BDP-architecture-overview.md) (pipeline
chạy thế nào) → [`guide/run-all.md`](guide/run-all.md) (**dựng cả stack và dùng thử**).

---

## Chỉ muốn chạy thử?

→ **[`guide/run-all.md`](guide/run-all.md)** — hướng dẫn đầu-cuối đầy đủ: khởi động stack Docker,
mở link nào, đăng ký connector, chạy Flink/Spark job, kiểm tra dữ liệu ở từng tầng, và dừng/reset.
Đây là guide vận hành chính; các file `guide/*.md` còn lại đi sâu theo từng công cụ.

---

## File vận hành (ở thư mục gốc, không nằm trong `docs/`)

| File | Là gì |
|---|---|
| [`../README.md`](../README.md) | Giới thiệu dự án, tech stack, quick start, ảnh chụp màn hình. Bản "mặt tiền" của repo. |
| [`../docker-compose.yml`](../docker-compose.yml) | Định nghĩa toàn bộ 21 service. Nguồn sự thật về port, image, env. |
| `../.env` | Secrets local (**không commit** — đã có trong `.gitignore`). Xem [`infra/infra.md`](infra/infra.md) §4. |

---

## `architecture/` — nguồn sự thật về thiết kế

Đọc trọn bài liên quan trước khi đổi kiến trúc, mô hình dữ liệu, hoặc logic xử lý.

| Tài liệu | Nội dung |
|---|---|
| [`architecture/BDP-architecture-overview.md`](architecture/BDP-architecture-overview.md) | Luồng đầu-cuối, 4 lane xử lý, kiểm kê 21 service, ranh giới trách nhiệm giữa các công cụ. |
| [`architecture/BDP-data-model.md`](architecture/BDP-data-model.md) | 4 thực thể nguồn, đặc tính CDC/replica identity, 4 bảng metric ClickHouse, các tầng lakehouse. |
| [`architecture/BDP-streaming-lanes.md`](architecture/BDP-streaming-lanes.md) | Lane 1 (dashboard metrics) và Lane 3 (fraud detection) — window, watermark, state, checkpoint. |
| [`architecture/BDP-lakehouse-medallion.md`](architecture/BDP-lakehouse-medallion.md) | Bronze → Silver → Gold → Iceberg: đường dẫn S3, dedup, join, snapshot/time-travel. |
| [`architecture/BDP-current-state.md`](architecture/BDP-current-state.md) | **Hệ thống *đang* ra sao** — đánh giá độ trưởng thành, audit "metadata sprawl", các khoảng trống đã biết. Đọc trước khi lập kế hoạch cải tiến. |

## `guide/` — cách chạy từng phần

| Tài liệu | Nội dung |
|---|---|
| [`guide/run-all.md`](guide/run-all.md) | **Guide chính** — dựng và vận hành cả stack (setup, chạy, kiểm tra, reset, troubleshooting). |
| [`guide/metadata-control-plane.md`](guide/metadata-control-plane.md) | **Sinh artifact từ metadata** — `dataplatform.cli check/show/write`, cách thêm dataset, contract có những trường gì. |
| [`guide/cdc-and-connectors.md`](guide/cdc-and-connectors.md) | Đăng ký Debezium source + 5 ES sink + S3 sink; cách sửa/xóa connector; publication & replication slot. |
| [`guide/flink-jobs.md`](guide/flink-jobs.md) | Submit/huỷ job Flink, đọc Web UI, checkpoint, và vì sao chỉ nên chạy `lane1_dashboard`. |
| [`guide/spark-lakehouse.md`](guide/spark-lakehouse.md) | Chạy 3 batch job (enrich → gold → iceberg), packages cần thiết, thứ tự phụ thuộc. |
| [`guide/clickhouse-grafana.md`](guide/clickhouse-grafana.md) | Khởi tạo schema thủ công (init **không** tự chạy), kiểm tra Kafka engine/MV, dựng Grafana. |
| [`guide/kibana.md`](guide/kibana.md) | Data view, saved search, dashboard điều tra fraud/failed transaction. |
| [`guide/trino.md`](guide/trino.md) | Truy vấn liên nguồn Postgres + ClickHouse + Iceberg từ một SQL engine. |
| [`guide/dlq-and-notifier.md`](guide/dlq-and-notifier.md) | **Luồng DLQ** — lỗi connector → `dlq.*` → phân loại → ClickHouse; cách truy vấn, vì sao không tự phát lại. (fraud-notifier vẫn còn nợ một bảng.) |

## `infra/` — stack local

| Tài liệu | Nội dung |
|---|---|
| [`infra/infra.md`](infra/infra.md) | 21 service, bảng port đầy đủ, volume, network, biến môi trường, yêu cầu tài nguyên. |

## `roadmap/` — hướng đi

| Tài liệu | Nội dung |
|---|---|
| [`roadmap/BDP-metadata-driven-roadmap.md`](roadmap/BDP-metadata-driven-roadmap.md) | Lộ trình 9 pha chuyển sang metadata-driven: kiến trúc đích, mô hình metadata, ví dụ contract, rủi ro, ước lượng. |

## `reference/` — sổ tay dùng lại (không gắn dự án này)

| Tài liệu | Nội dung |
|---|---|
| [`reference/metadata-driven-greenfield.md`](reference/metadata-driven-greenfield.md) | **Triển khai metadata-driven cho dự án mới** — khi nào nên/không nên, yêu cầu tiên quyết, kiến trúc, 4 quyết định đắt-nếu-sai, bậc thang trưởng thành, checklist ngày-0. |

## `decisions/` — Architecture Decision Records (ADR)

Đánh số, chỉ thêm mới. Không sửa quyết định cũ — viết ADR mới để **supersede** nó. Viết ADR mới cho
mọi thay đổi về ranh giới trách nhiệm công cụ, mô hình dữ liệu, hoặc đảo ngược một quyết định trước
đó. Dùng [`decisions/template.md`](decisions/template.md).

| ADR | Quyết định |
|---|---|
| [0001](decisions/0001-record-architecture-decisions.md) | Ghi lại quyết định kiến trúc dưới dạng ADR. |
| [0002](decisions/0002-cdc-via-debezium-pgoutput.md) | CDC bằng Debezium + `pgoutput`, publication khai báo tường minh (không `FOR ALL TABLES`). |
| [0003](decisions/0003-avro-with-schema-registry.md) | Avro + Schema Registry cho CDC; `decimal.handling.mode=string`. |
| [0004](decisions/0004-replica-identity-full-for-mutable-tables.md) | `REPLICA IDENTITY FULL` cho `accounts`/`transfers`; DEFAULT cho phần còn lại. |
| [0005](decisions/0005-kafka-kraft-single-node.md) | Kafka KRaft single-node, RF=1, `auto.create.topics=true` — phạm vi lab. |
| [0006](decisions/0006-one-flink-job-per-lane-statement-set.md) | Một job Flink cho mỗi lane bằng `StatementSet`; các `lane1_*.py` rời là **di sản**. |
| [0007](decisions/0007-clickhouse-kafka-engine-serving.md) | ClickHouse serving qua Kafka engine + Materialized View, `ReplacingMergeTree` + TTL. |
| [0008](decisions/0008-medallion-on-minio-parquet.md) | Medallion (Bronze/Silver/Gold) dạng Parquet trên MinIO, ranh giới stream/batch. |
| [0009](decisions/0009-iceberg-rest-catalog.md) | Iceberg qua REST catalog cho snapshot/time-travel. |
| [0010](decisions/0010-trino-for-federation.md) | Trino làm lớp truy vấn liên nguồn. |
| [0011](decisions/0011-es-sink-upsert-by-primary-key.md) | ES sink upsert theo PK (`unwrap` + `extractKey`), `schema.ignore=true`. |
| [0012](decisions/0012-dlq-processor-not-wired.md) | DLQ processor tồn tại nhưng **chưa được nối** — ghi lại nợ kỹ thuật, không giả vờ nó chạy. *(Phần DLQ superseded bởi 0017; phần fraud-notifier vẫn đúng.)* |
| [0013](decisions/0013-secrets-in-gitignored-env.md) | Secrets nằm trong `.env` đã gitignore; đây là **giới hạn đã biết**, không phải rò rỉ Git. |
| [0014](decisions/0014-adopt-metadata-driven-roadmap.md) | Chọn hướng metadata-driven làm đích kiến trúc (theo `roadmap/`). |
| [0015](decisions/0015-metadata-registry-yaml-first.md) | **Metadata là YAML trong Git**; generator sinh artifact; `check` so ngữ nghĩa làm cửa gác. Lát cắt dọc đầu tiên: 5/5 ES sink khớp tuyệt đối. |
| [0016](decisions/0016-rename-platform-to-dataplatform.md) | Thư mục control plane là `dataplatform/`, **không** `platform/` (trùng module stdlib Python). *(Sửa đề xuất trong roadmap §2.3.)* |
| [0017](decisions/0017-dlq-flow-observe-then-park.md) | **Nối DLQ**: mọi sink bật `errors.tolerance=all` + DLQ → processor phân loại → `dlq.events` → ClickHouse. **Không tự động phát lại** (replay về topic gốc làm sai metric). *(Supersedes 0012 phần DLQ.)* |
| [0018](decisions/0018-generate-debezium-and-publication.md) | **Sinh Debezium connector + publication SQL từ một nguồn** → `table.include.list` và `FOR TABLE` không thể lệch nhau (diệt sprawl #2/#3). `check` học so artifact dạng text (SQL). |
| [0019](decisions/0019-generate-clickhouse-metric-ddl.md) | **Sinh DDL ClickHouse từ metric contract** — 3 đối tượng/metric từ một `columns` → hết cảnh MV bỏ dữ liệu âm thầm (sprawl #8/#9). Kiểm chứng bằng cách áp DDL thật vào ClickHouse: **12/12 giống hệt**. Lộ ra bất nhất `kafka_max_block_size` trong bản viết tay. |
