# ADR-0005: Kafka KRaft single-node, RF=1, auto-create topic — phạm vi lab

- **Status:** Accepted *(chỉ cho lab — chặn lên production)*
- **Date:** 2026-07-15 *(hồi tố)*
- **Deciders:** Phan Trường

## Bối cảnh

Toàn bộ stack phải chạy trên **một laptop** với 10–12 GB RAM cấp cho Docker, cùng lúc với 19 container
khác. Kafka đúng chuẩn production cần tối thiểu 3 broker cộng ZooKeeper (hoặc 3 KRaft controller) —
riêng nó đã ăn hết ngân sách RAM.

## Quyết định

Chạy Kafka **một node ở chế độ KRaft**, node đó vừa là broker vừa là controller, mọi replication
factor = 1, và bật auto-create topic.

```yaml
KAFKA_PROCESS_ROLES: "broker,controller"
KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"   # cho Debezium tự tạo topic
KAFKA_HEAP_OPTS: "-Xmx512M -Xms512M"
```

Hai listener: `kafka:9092` cho service nội bộ, `localhost:29092` cho công cụ chạy trên host.

## Hệ quả

**Dễ hơn:**
- Kafka gói gọn trong 512 MB heap, chừa RAM cho Flink/Spark/ES.
- KRaft bỏ hẳn ZooKeeper — bớt một service.
- Auto-create nghĩa là Debezium và Flink cứ ghi, không cần bước tạo topic thủ công.

**Khó hơn / phải chấp nhận — đây là các điểm chặn production:**

| Điểm | Rủi ro |
|---|---|
| **RF=1** | Broker chết là **mất dữ liệu**. Không có bản sao. |
| **Không có volume cho Kafka** | `docker compose down` xoá sạch mọi topic và offset. |
| **Single controller** | Không có quorum. Node chết là cụm chết. |
| **`auto.create.topics=true`** | Topic sinh ra với partition/retention mặc định, **không kiểm soát**. Gõ sai tên topic → tạo ra topic rác thay vì báo lỗi. |
| **PLAINTEXT, không auth** | Ai vào được network là đọc/ghi được mọi topic. |

Cái `auto.create.topics` còn che một lớp lỗi cụ thể: `dlq-processor` subscribe 6 topic `dlq.*`
**không bao giờ có message**, nhưng vì auto-create, chúng được tạo rỗng và service báo "Connected,
monitoring 6 topics" — trông như khoẻ. Xem [ADR-0012](0012-dlq-processor-not-wired.md).

**Đường lên production** (nằm ở Pha 0 và Pha 2 của [lộ trình](../roadmap/BDP-metadata-driven-roadmap.md)):
1. Multi-broker, RF≥3, `min.insync.replicas=2`
2. **Tắt** `auto.create.topics`; quản lý topic bằng manifest sinh từ metadata (partition/retention/RF
   theo env)
3. Thêm volume cho log Kafka
4. Bật SASL/TLS + ACL

## Phương án đã cân nhắc

- **3 broker trên cùng máy.** Bị loại: ~1.5 GB RAM chỉ cho Kafka, và **vẫn không** có HA thật — cùng
  một host thì cùng một điểm chết. Trả giá thật để lấy độ an toàn giả.
- **Redpanda.** Bị loại: nhẹ hơn và tương thích API Kafka, nhưng ClickHouse Kafka engine, Debezium và
  connector Flink đều được kiểm chứng kỹ nhất với Kafka thật. Mục tiêu học tập là *chính* Kafka.
- **KRaft với controller tách riêng.** Bị loại: thêm container mà không thêm khả năng chịu lỗi khi vẫn
  chỉ một máy.
