# ADR-0020: Sinh bản kê topic Kafka từ registry — bịt khoảng trống production #8

- **Status:** Accepted
- **Date:** 2026-07-18
- **Deciders:** Phan Trường

## Bối cảnh

Kafka đang bật `auto.create.topics.enable=true` (docker-compose.yml). Hễ có ai produce hoặc consume
một topic chưa tồn tại, broker **lặng lẽ** tạo nó với partition/retention/RF mặc định. Tiện lúc dựng
lab — Debezium và Flink cứ ghi là topic tự có — nhưng ở production đây là khoảng trống #8 trong
[`BDP-current-state.md`](../architecture/BDP-current-state.md) §4.2:

- Gõ sai tên topic → tạo ra **topic rác** thay vì báo lỗi. Không có lưới chặn.
- Không ai kiểm soát số partition (trần song song) hay retention. Mọi topic ăn mặc định broker.
- Câu hỏi "hệ thống có những topic nào" **không có nguồn sự thật** — phải đi hỏi broker đang chạy.

Đây cũng là mẩu sprawl còn lại ở tầng ingestion: danh sách topic là một "sự thật về hệ thống" chưa
được khai ở đâu tập trung. Nó chỉ vừa **mở khoá** được: [ADR-0019](0019-generate-clickhouse-metric-ddl.md)
đưa nốt 4 metric dataset vào registry, nên giờ registry đã biết **đủ** mọi topic dữ liệu.

## Quyết định

Khai mọi topic **một lần** trong registry (hoặc suy ra từ nó), rồi sinh hai artifact từ **cùng một
nguồn**:

- `kafka/topics.json` — **bản kê khai báo**, máy đọc được, diff được. Đây là "topic manifest" mà roadmap
  Pha 2 gọi tên.
- `kafka/create-topics.sh` — **script tạo topic idempotent** (`--if-not-exists`), chạy được trong image
  `cp-kafka`. Đây là dạng *thực thi* của cùng bản kê.

Cả hai sinh từ một hàm `_entries()` nên **không thể lệch nhau**.

### Ba nguồn topic — và ranh giới "suy diễn" vs "khai báo"

Đây là chỗ tinh tế nhất của thiết kế:

| Nhóm | Ví dụ | Từ đâu ra |
|---|---|---|
| **dataset** | `bankdb.public.transactions`, `metrics.timeseries`, `fraud-alerts` | Suy thẳng từ `source.topic` của mỗi dataset. Thêm dataset = thêm topic, **không đụng** file generator. |
| **dlq** | `dlq.es-sink-transactions` | **Tái dùng** danh sách của `generators/dlq.py`, không tự liệt kê lại. |
| **infra** | `dlq.events`, `_connect_*`, `_schemas` | Không gắn dataset nào → **khai tay** dưới dạng hằng số có giải thích. |

Điểm mấu chốt: nhóm dataset + dlq là **sự thật suy diễn**; nhóm infra là **sự thật khai báo**. Tôi nói
rõ ranh giới này trong code thay vì trộn lẫn, để người đọc biết chỗ nào tự cập nhật theo registry, chỗ
nào phải sửa tay khi thêm hạ tầng mới.

Nếu tự liệt kê lại danh sách DLQ ở đây thay vì tái dùng `dlq.py`, tôi sẽ đẻ ra **đúng thứ sprawl đang
diệt**: hai nơi cùng khai một danh sách, lệch nhau lúc nào không biết.

### Nguyên tắc "tái tạo hiện trạng trước" (strangler-fig)

Bản kê **đầu tiên** phải mô tả đúng những gì auto-create *đang* tạo ra, để khi tắt auto-create thì
**không có gì đổi**. Hệ quả cụ thể:

- **RF = 1** (single node, [ADR-0005](0005-kafka-kraft-single-node.md)). Khoá ở một hằng số, lên
  multi-broker thì đổi theo env.
- **Partition = mặc định hiện tại** (data = 1; `_connect_offsets` = 25, `_connect_status` = 5,
  `_connect_configs`/`_schemas` = 1). Tăng partition cho `transactions` (throughput cao) là quyết định
  hiệu năng **có chủ ý về sau** — nó đổi `key_by` của fraud detector, không lén nhét vào đây.
- **Chỉ khai `cleanup.policy` cho topic compacted.** Topic `delete` để `configs` **rỗng**, KHÔNG khai
  `cleanup.policy=delete` — vì đó đã là mặc định broker, và topic thật do auto-create sinh ra **không có
  override này** (cột Configs trống khi `describe`). Khai thừa sẽ làm bản kê lệch với hiện trạng và mất
  khả năng đối chiếu sạch. Retention cũng để mặc định broker vì cùng lý do.

Nói cách khác: bản kê là **ảnh chụp trung thực** của cluster, không phải nơi tôi lén cải tiến. Cải tiến
(partition, retention DLQ dài hơn) là các thay đổi riêng, có đối chiếu riêng.

## Kiểm chứng — đối chiếu bản kê với Kafka **thật**

Giống oracle ClickHouse ở ADR-0019: không tin bản kê cho tới khi so nó với cluster đang chạy.

1. `kafka-topics --describe` toàn bộ cluster → lấy `(tên, partitions, RF, cleanup.policy)` thật.
2. So từng topic thật với bản kê.

```
=== A. Moi topic THAT (tru broker-owned) phai co trong manifest & khop ===
  [KHỚP] _connect_configs    p=1  rf=1 policy=compact
  [KHỚP] _connect_offsets    p=25 rf=1 policy=compact
  [KHỚP] _connect_status     p=5  rf=1 policy=compact
  [KHỚP] _schemas            p=1  rf=1 policy=compact
  [KHỚP] dlq.es-sink-*       p=1  rf=1 policy=delete   (×5)
  [KHỚP] dlq.s3-sink-cdc     p=1  rf=1 policy=delete
  [KHỚP] fraud-alerts        p=1  rf=1 policy=delete
  => ✅ 11/11 topic thật khớp bản kê tuyệt đối, 0 lệch/mồ côi.
```

`__consumer_offsets` bị loại đúng cách: **broker tự quản** nó bất kể `auto.create.topics`, không phải
thứ control plane khai hay xoá được.

Chín topic còn lại trong bản kê (`bankdb.public.*`, `metrics.*`, `dlq.events`) **chưa xuất hiện** phiên
này vì producer chưa chạy (0 connector đăng ký, không Flink job). Để không phải dựng cả pipeline chỉ để
kiểm tra tên, tôi làm **cross-check tĩnh**: mọi topic mà *artifact sinh khác* tham chiếu tới phải nằm
trong bản kê.

```
  bankdb.public.{accounts,customers,transactions,transfers}  <- es-sink + debezium(topic.prefix)  KHỚP
  dlq.*                                                        <- errors.deadletterqueue.topic.name  KHỚP
  => ✅ bản kê phủ HẾT topic mà pipeline dùng.
```

Topic CDC được **kiểm chứng kép**: es-sink tham chiếu chúng, *và* `topic.prefix + schema + table` của
Debezium suy ra đúng cùng tên. Topic metric suy từ cùng `source.topic` mà bảng Kafka engine ClickHouse
(`kafka_topic_list`, ADR-0019) đang đọc — nhất quán theo cấu tạo.

## Phát hiện: bản kê bản đầu **sót `_schemas`**

Bước đối chiếu live lôi ra ngay một lỗi tôi **không lường trước**: `_schemas` — store schema của
Confluent Schema Registry (single-partition, compacted) — tồn tại trên cluster nhưng **không có** trong
bản kê đầu. Tôi đã liệt kê `_connect_*` mà quên `_schemas`.

Đây đúng loại topic hạ tầng mà roadmap Pha 2 cảnh báo *"còn thiếu các topic nội bộ trước khi dám tắt
auto.create.topics"*. Nếu tắt auto-create mà thiếu `_schemas` trong script tạo, Schema Registry sẽ
**không khởi động được** → sập toàn bộ đường CDC Avro. Đã bổ sung. Giá trị của "đối chiếu với thật":
nó bắt lỗi mà suy luận thuần không bắt được — y như vụ `kafka_max_block_size` ở ADR-0019.

## Hệ quả

**Dễ hơn:**
- Có **nguồn sự thật** cho topic: câu hỏi "hệ thống có topic nào, cấu hình ra sao" trả lời bằng đọc một
  file, không phải hỏi broker.
- Thêm dataset = topic của nó **tự vào** bản kê và script. Thêm connector = topic DLQ **tự vào** (qua
  `dlq.py`).
- Có đường đi rõ ràng để **tắt `auto.create.topics`** an toàn (xem dưới).
- Control plane phủ **13 artifact**; `check` gác cả 13.

**Khó hơn / phải chấp nhận:**
- `kafka/topics.json` + `create-topics.sh` giờ là **file sinh** — sửa phải sửa registry/generator (có
  header cảnh báo). Nhóm infra khai tay trong `topic_manifest.py`, không suy từ dataset.
- Bản kê **chưa được áp** và auto-create **vẫn bật**. Đây là cố ý (xem dưới), nhưng nghĩa là giá trị
  "kiểm soát topic" chưa hiện thực hoá tới khi làm nốt các bước gated.

## Việc còn lại (các cổng có chủ ý, chưa làm trong ADR này)

Tôi **không** tắt `auto.create.topics` ngay, đúng kỷ luật strangler-fig "thêm cái mới song song, kiểm
chứng, rồi mới bỏ cái cũ":

1. **Nối `create-topics.sh` vào vòng khởi động** (service `kafka-init` trong compose, hoặc chạy tay).
   Kiểm chứng: mọi topic trong bản kê tồn tại sau khi chạy.
2. **Chạy pipeline đầy đủ** (đăng ký Debezium + deploy Flink) để 9 topic "chờ" xuất hiện, rồi đối chiếu
   lại — đóng nốt phần tên topic CDC/metric bằng bằng chứng live thay vì cross-check tĩnh.
3. **Chỉ khi (1)(2) xanh** mới đặt `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`. Đây là bước bỏ "cái cũ".

Giữ auto-create bật tới lúc đó nghĩa là nếu script tạo topic sót cái gì, hệ thống vẫn chạy — lưới an
toàn không rút trước khi lưới mới được chứng minh.

## Phương án đã cân nhắc

- **Tắt `auto.create.topics` ngay trong ADR này.** Bị loại: rút lưới an toàn trước khi kiểm chứng script
  tạo topic phủ đủ. Vụ `_schemas` cho thấy suy luận thuần **sẽ** sót topic — tắt vội là tự tạo sự cố.
- **Tự liệt kê lại danh sách DLQ trong `topic_manifest.py`.** Bị loại: tạo ra đúng thứ sprawl đang diệt.
  Tái dùng `dlq.py`.
- **Khai `cleanup.policy=delete` cho mọi topic cho "tường minh".** Bị loại: làm bản kê lệch với topic
  thật (vốn không có override), phá kiểm chứng sạch. Chỉ khai cái khác mặc định.
- **Sinh thẳng service `kafka-init` + tắt auto-create luôn.** Bị hoãn: đó là thay đổi compose chưa
  kiểm chứng được offline; tách thành bước gated ở trên.
- **Chỉ sinh JSON, bỏ script `.sh`.** Bị loại: bản kê không ai áp được chỉ là tài liệu. Script là thứ
  biến khai báo thành hành động, và là cơ chế để tắt auto-create.
- **Chỉ sinh `.sh`, bỏ JSON.** Bị loại: script khó cho máy đọc (catalog/deployer tương lai cần dữ liệu
  có cấu trúc). Giữ cả hai, cùng một nguồn.
