# Thư viện bên thứ ba đi kèm repo

Các file `.jar` trong thư mục này **không phải mã nguồn của dự án**. Chúng là bản phát hành nhị phân
của bên thứ ba, được commit kèm để job Flink chạy được ngay mà không cần tải lúc submit (container
Flink mount thư mục này vào `/opt/flink/jobs/jars`, xem `flink/jobs/metric_runner.py`).

Cả bốn đều theo **Apache License 2.0**. Giấy phép đó cho phép redistribute, với điều kiện giữ thông
báo bản quyền và ghi nguồn — đó là mục đích của file này.

| File | Dự án | Version | Giấy phép | Nguồn |
|---|---|---|---|---|
| `flink-sql-connector-kafka-3.1.0-1.18.jar` | Apache Flink — Kafka connector | 3.1.0-1.18 | Apache-2.0 | https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/ |
| `flink-sql-avro-confluent-registry-1.18.1.jar` | Apache Flink — Avro + Confluent Schema Registry | 1.18.1 | Apache-2.0 | https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/ |
| `flink-connector-jdbc-3.1.2-1.18.jar` | Apache Flink — JDBC connector | 3.1.2-1.18 | Apache-2.0 | https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/ |
| `clickhouse-jdbc-0.6.3-all.jar` | ClickHouse JDBC driver | 0.6.3 (shaded `-all`) | Apache-2.0 | https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/ |

Bản quyền thuộc về các tác giả tương ứng (Apache Software Foundation; ClickHouse, Inc.). Toàn văn
Apache License 2.0: https://www.apache.org/licenses/LICENSE-2.0

## Vì sao commit nhị phân vào git

Đây là **đánh đổi có chủ ý**, không phải sơ suất:

- **Được:** `docker compose up` là chạy được, không phụ thuộc mạng hay Maven lúc submit job — quan
  trọng với một repo dùng để học và demo.
- **Mất:** `git clone` nặng thêm ~47 MB cho thứ ai cũng tải được từ Maven, và git không diff được
  nhị phân.

Muốn đổi hướng: xoá thư mục này khỏi git, thêm vào `.gitignore`, và viết một script tải từ các URL
Maven ở bảng trên trước khi submit job. Khi đó `git clone` còn ~3 MB, đổi lại thêm một bước setup.
