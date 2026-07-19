# ADR-0029: Encode connection non-Trino — registry là nguồn endpoint duy nhất (Pha 6)

- **Status:** Accepted — 7 connection trong registry (3 Trino + 4 non-Trino), 4 generator + 1 deployer đọc từ đó, `check` 18/18 byte-exact
- **Date:** 2026-07-19
- **Deciders:** Phan Trường

## Bối cảnh

[ADR-0025](0025-connection-registry-trino-catalog.md) mới encode **3 connection có Trino** (postgres,
clickhouse, iceberg) và tự nhận nợ còn lại: "kafka, es, s3, schema-registry chưa vào registry". Mà các
hệ thống này vẫn được kết nối tới — chỉ là endpoint của chúng **treo rải rác, hardcode trong từng
generator**:

- `es_sink.py`  → `${env:ELASTICSEARCH_URL}`
- `s3_sink.py`  → `${env:S3_ENDPOINT}`, `S3_BUCKET_BRONZE`, `S3_REGION`, `S3_ACCESS_KEY`…
- `debezium.py` → `${env:POSTGRES_HOST/PORT/DB}`, `REPLICATION_USER/PASSWORD` (Postgres đã có connection
  cho Trino, nhưng phần CDC thì hardcode lại — **trùng nguồn**)
- `${env:SCHEMA_REGISTRY_URL}` lặp ở **cả 3** connector generator
- `topic_manifest.py` → `kafka:9092`; `flink_metrics.py` → `kafka:9092` + `http://schema-registry:8081`

Đổi một host = sửa nhiều file. "Connection non-Trino" vẫn là **tên treo** — đúng thứ registry sinh ra để diệt.

## Quyết định

**Registry là nguồn endpoint duy nhất, kể cả cho consumer ngoài Trino.**

1. Thêm khối tự do `endpoints` (map `key -> chuỗi`) vào `connection.schema.json` — cho placeholder mà
   consumer NGOÀI Trino cần. Không mô hình hoá sâu (giữ đúng lý luận ADR-0024/0025: property đặc thù từng
   consumer, carry map trực tiếp). Vẫn **không secret** — chỉ `${env:...}` / literal host:port.
2. Thêm 4 connection: `kafka`, `schema_registry`, `elasticsearch_serving`, `s3_minio`; và bổ sung khối
   `endpoints` cho `postgres_main` (phần CDC). Nay **mọi hệ thống kết nối đều là contract hạng nhất** —
   đóng nốt nợ Pha 1.
3. Sửa generator đọc endpoint **TỪ** connection thay vì hardcode: `es_sink`, `s3_sink`, `debezium`,
   `topic_manifest`, và deployer `flink_metrics`. Helper `registry.endpoint(conns, name, key)` tra cứu,
   **thiếu thì ném `ContractError`** — thà đứt lúc sinh còn hơn sinh config trỏ sai âm thầm.

### Vì sao hai dạng endpoint cho cùng một hệ thống

`schema_registry` mang cả `connect_url: "${env:SCHEMA_REGISTRY_URL}"` (Kafka Connect resolve env qua
`EnvVarConfigProvider` trong container) **và** `url: "http://schema-registry:8081"` (Flink nhúng literal,
không có lớp config provider). Postgres tương tự: Trino dùng JDBC url, Debezium dùng `connect_hostname/port`
với **role replication riêng**. Registry giờ **phơi bày** đúng thực tế "một hệ thống, nhiều lớp consumer,
nhiều cách resolve" — thay vì giấu nó trong code từng generator.

## Kiểm chứng

- **`check` 18/18 byte-exact.** Refactor trong suốt: chuỗi sinh y hệt bản cũ, chỉ đổi *nguồn* (registry
  thay vì literal). Đây là bằng chứng wiring đúng — nếu lệch một ký tự, oracle đỏ ngay.
- `connectors.desired_connectors()` và `flink_metrics.{BOOTSTRAP,SCHEMA_REGISTRY}` resolve ra đúng giá trị cũ.
- Connection registry lên **7 entry** (3 Trino + 4 non-Trino), validate qua JSON Schema.

## Hệ quả

**Dễ hơn:** đổi host/cổng/bucket/role của bất kỳ hệ thống nào = sửa **một** file connection. Catalog/lineage
(và sau này DataHub/OpenMetadata) có thể đọc registry để biết "nền tảng nối tới những hệ thống nào, bằng gì".
Nợ Pha 1 (mã hoá connection) **đóng hoàn toàn**.

**Khó hơn / phải chấp nhận:**
- `endpoints` là map tự do — key là quy ước (vd `connect_url` vs `url`), không có schema chặt cho từng key.
  Đánh đổi có chủ đích: đúng như property Trino, ép khuôn sẽ đẻ mapping khổng lồ. Guard `endpoint()` bù lại
  bằng lỗi rõ khi thiếu key.
- Connection non-Trino không sinh artifact riêng (không như Trino catalog) — chúng là **nguồn tra cứu**
  cho generator khác. Giá trị nằm ở "một nguồn sự thật", không phải thêm file sinh.

## Phương án đã cân nhắc

- **Chỉ thêm entry registry trơ, giữ generator hardcode.** Loại: đó là 🟡 nửa vời — endpoint vẫn định nghĩa
  hai nơi, chưa đóng sprawl.
- **Mô hình hoá sâu từng loại endpoint thành field.** Loại: cùng lý do ADR-0024/0025 — carry map trực tiếp.
- **Nhét secret thật vào `endpoints`.** Loại dứt khoát: luôn `${env:...}`, resolve ở runtime.
