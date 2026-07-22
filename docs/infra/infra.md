# infra — stack Docker Compose local

> 21 service, bảng port đầy đủ, volume, network, và 48 biến môi trường.
> Nguồn: [`docker-compose.yml`](../../docker-compose.yml).
> Cách dựng: [`../guide/run-all.md`](../guide/run-all.md).
> Cập nhật lần cuối: 2026-07-15.

---

## 1. Yêu cầu tài nguyên

| Thành phần | Khuyến nghị |
|---|---|
| RAM cấp cho Docker | **10–12 GB** (dưới 8 GB sẽ có service bị OOM-kill) |
| CPU | 4 core trở lên |
| Disk trống | 30 GB |
| OS | Windows + Docker Desktop WSL2, hoặc Linux/macOS |

Giới hạn heap đã đặt sẵn để vừa laptop: Kafka 512 MB, Kafka Connect 512 MB, Elasticsearch 512 MB,
Flink JobManager 1024 MB, mỗi TaskManager 1280 MB, Spark worker 2 GB.

---

## 2. Bảng port

Cột "Trong container" là thứ để dùng khi service **này** gọi service **kia** trong network
`bigdata-net` — thường khác cổng bạn mở trên browser.

| Service | Host | Trong container | Ghi chú |
|---|---|---|---|
| postgres | `5432` | `postgres:5432` | |
| kafka | `29092` | `kafka:9092` | Host dùng `localhost:29092`, service nội bộ dùng `kafka:9092` |
| kafka (controller) | — | `kafka:9093` | KRaft, không map ra host |
| schema-registry | `8081` | `schema-registry:8081` | |
| kafka-connect | `8083` | `kafka-connect:8083` | REST API |
| kafka-ui | `8080` | — | |
| **flink jobmanager** | **`8082`** | `jobmanager:8081` | **Remap** — 8081 đã dùng cho Schema Registry |
| clickhouse (HTTP) | `8123` | `clickhouse:8123` | |
| clickhouse (native) | **không map** | `clickhouse:9000` | Grafana plugin dùng cổng này |
| minio (S3 API) | `9000` | `minio:9000` | |
| minio (Console) | `9001` | — | |
| grafana | `3000` | — | |
| elasticsearch | `9200` | `elasticsearch:9200` | |
| kibana | `5601` | — | |
| **spark-master** | **`8090`** | `spark-master:8080` | Remap tránh đụng Kafka UI |
| spark-master (RPC) | `7077` | `spark-master:7077` | |
| **spark-worker** | **`8091`** | `spark-worker:8081` | |
| iceberg-rest | `8181` | `iceberg-rest:8181` | |
| **trino** | **`8085`** | `trino:8080` | |

> Ba cổng bị remap (`8082`, `8090`, `8085`) là chỗ dễ nhầm nhất. Quy tắc: **UI trên browser dùng cổng
> host; config giữa các service dùng tên service + cổng trong container.**

---

## 3. Volume & network

**Network:** `bigdata-net` (bridge) — mọi service nằm chung, gọi nhau bằng tên service.

| Volume | Chứa gì | Mất khi `down -v` |
|---|---|---|
| `bigdata_postgres_data` | Dữ liệu OLTP + replication slot | Toàn bộ |
| `bigdata_clickhouse_data` | Bảng metric | Phải chạy lại init |
| `bigdata_clickhouse_logs` | Log ClickHouse | |
| `bigdata_minio_data` | **Toàn bộ lakehouse** + checkpoint Flink | Bronze/Silver/Gold/Iceberg |
| `bigdata_elasticsearch_data` | Index tìm kiếm | Sink sẽ index lại |
| `bigdata_grafana_data` | Dashboard, datasource | Phải dựng lại tay |
| `bigdata_flink_checkpoints` | Checkpoint local | |

**Kafka không có volume** — dữ liệu topic nằm trong lớp ghi của container. `docker compose down` là
mất mọi message và offset. Có chủ ý cho lab; production thì bắt buộc phải có volume.

**Bind mount** (sửa trên host là có hiệu lực ngay, không cần rebuild):

| Host | Container | Dùng cho |
|---|---|---|
| `./postgres/init` | `/docker-entrypoint-initdb.d` | Chạy **tự động** lúc khởi tạo DB |
| `./flink/jobs` | `/opt/flink/jobs` | Sửa `.py` → submit lại là xong |
| `./spark/jobs` | `/opt/spark-jobs` | Như trên |
| `./trino/etc/catalog` | `/etc/trino/catalog` | Catalog Trino |
| `./trino/etc/jvm.config`, `config.properties` | `/etc/trino/...` | **Mount kiểu file** — xem cảnh báo dưới |

> **`clickhouse/init` không được mount.** Khác với `postgres/init`, schema ClickHouse **không** tự
> chạy. Xem [`../guide/clickhouse-grafana.md`](../guide/clickhouse-grafana.md) §1.

> **Bẫy `jvm.config` trên Windows.** `./trino/etc/jvm.config` được mount **kiểu file**. Nếu file
> chưa tồn tại trên host, Docker sẽ tạo một **folder** trùng tên, và Trino fail với
> `Are you trying to mount a directory onto a file or vice-versa?`. Xử lý: xoá cái folder đó, tạo lại
> đúng dạng file, rồi `docker compose up -d trino`.

---

## 4. Biến môi trường

Compose tham chiếu **48 biến** và **không có giá trị mặc định** — thiếu là service không lên. Tạo
`.env` ở thư mục gốc.

> `.env` nằm trong [`.gitignore`](../../.gitignore) và **chưa từng được commit** (kiểm chứng:
> `git log --all -- .env` không có kết quả). Giữ nguyên như vậy. Xem
> [ADR-0013](../decisions/0013-secrets-in-gitignored-env.md).

### 4.1 Kết nối & thông tin đăng nhập

| Biến | Ví dụ | Dùng bởi |
|---|---|---|
| `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | `bankdb` / `admin` / *secret* | postgres, trino |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `postgres` / `5432` | connect, trino, generator |
| `REPLICATION_USER` / `REPLICATION_PASSWORD` | `replicator` / *secret* | Debezium (**chỉ user này**) |
| `APP_USER` / `APP_PASSWORD` | `bankapp` / *secret* | generator (không dùng user admin) |
| `CLICKHOUSE_DB` / `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | `metrics` / `admin` / *secret* | clickhouse, trino, dlq, notifier |
| `CLICKHOUSE_HOST` / `CLICKHOUSE_PORT` | `clickhouse` / `8123` | dlq, notifier, trino |
| `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT` | `1` | clickhouse |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | *secret* | minio, minio-init |
| `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` | *secret* | grafana |

Ba user Postgres tách vai trò rõ ràng — `admin` (DDL), `replicator` (chỉ CDC), `bankapp` (chỉ ghi dữ
liệu ứng dụng). Đây là thực hành tốt, giữ nguyên.

### 4.2 URL nội bộ

| Biến | Giá trị đúng | Bẫy |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | **Không** phải `localhost:29092` — đó là cổng cho host |
| `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` | |
| `KAFKA_CONNECT_URL` | `http://kafka-connect:8083` | |
| `ELASTICSEARCH_URL` | `http://elasticsearch:9200` | |
| `KIBANA_PUBLIC_BASE_URL` | `http://localhost:5601` | Đây **là** URL phía host (browser dùng) |
| `ICEBERG_REST_URI` | `http://iceberg-rest:8181` | |

### 4.3 S3 / MinIO

| Biến | Giá trị |
|---|---|
| `S3_ENDPOINT` | `http://minio:9000` |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | trùng `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` |
| `S3_REGION` | `us-east-1` |
| `S3_PATH_STYLE_ACCESS` | `true` (**bắt buộc với MinIO**) |
| `S3_SSL_ENABLED` | `false` |
| `ICEBERG_WAREHOUSE` | `s3a://data-lake-iceberg/warehouse` (Spark dùng `s3a://`) |
| `ICEBERG_CATALOG_WAREHOUSE` | `s3://data-lake-iceberg/warehouse` (REST catalog dùng `s3://`) |

> Hai biến warehouse **khác nhau ở scheme** (`s3a://` vs `s3://`) — không phải lỗi chính tả. Spark
> dùng S3A filesystem của Hadoop; iceberg-rest dùng AWS SDK.

### 4.4 Generator (mặc định trong `generator/config.py`)

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `TARGET_RPS` | `150` | Tốc độ nền |
| `PEAK_RPS` | `800` | Tốc độ lúc burst |
| `DURATION_SEC` | `900` | Tổng thời gian chạy (15 phút) |
| `BURST_PROBABILITY` | `0.015` | ~1.5%/giây → trung bình ~1 burst/70 giây |
| `BURST_DURATION_MAX` | `5.0` | Burst dài tối đa (giây) |
| `PROB_TRANSFER` | `0.20` | 20% là transfer (có lifecycle) |
| `PROB_FAILURE` | `0.05` | 5% giao dịch fail |
| `TRANSFER_DELAY_MIN` / `MAX` | `1.0` / `5.0` | Độ trễ hoàn tất transfer |
| `STATS_INTERVAL_SEC` | `10` | Chu kỳ in thống kê |

Tăng `PROB_FAILURE` để ép ra alert Failed Storm (cần **≥15 giao dịch failed/5 phút/account**).

### 4.5 Email (fraud-notifier)

| Biến | Ghi chú |
|---|---|
| `SMTP_HOST` / `SMTP_PORT` | Mặc định `smtp.gmail.com` / `587` (STARTTLS) |
| `EMAIL_FROM` / `EMAIL_TO` | `EMAIL_TO` nhận nhiều địa chỉ, ngăn bằng dấu phẩy |
| `EMAIL_PASSWORD` | Gmail cần **App Password**, mật khẩu thường không dùng được |

---

## 5. Ghi chú bảo mật

Đây là môi trường **lab/dev**. Những điểm sau là cố ý cho tiện, và **phải sửa trước khi lên production**:

| Hiện tại | Production cần |
|---|---|
| Elasticsearch `xpack.security.enabled=false` | Bật auth + TLS |
| Kafka PLAINTEXT, không auth | SASL/TLS + ACL |
| Trino, MinIO, Kafka UI, Iceberg REST không auth | Auth + phân quyền |
| Secret truyền qua biến môi trường (lộ trong `docker inspect`) | Docker secrets / Vault |
| Secret plaintext trong `.env` local | Secret manager + xoay vòng định kỳ |
| Kafka RF=1, ES single-node | Multi-broker RF≥3, ES cluster |
| `AUTO_CREATE_TOPICS_ENABLE=true` | Tắt; quản lý topic bằng khai báo |

Đánh giá đầy đủ: [`../architecture/BDP-current-state.md`](../architecture/BDP-current-state.md) §4.
