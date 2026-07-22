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
- **Chỉ khai `cleanup.policy` cho topic compacted.** Topic `delete` để `configs` **rỗng**, không khai
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
  => 11/11 topic thật khớp bản kê, 0 lệch/mồ côi.
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
  => bản kê phủ hết topic mà pipeline dùng.
```

Topic CDC được **kiểm chứng kép**: es-sink tham chiếu chúng, *và* `topic.prefix + schema + table` của
Debezium suy ra đúng cùng tên. Topic metric suy từ cùng `source.topic` mà bảng Kafka engine ClickHouse
(`kafka_topic_list`, ADR-0019) đang đọc — nhất quán theo cấu tạo.

## Phát hiện: đối chiếu-với-thật lôi ra **ba** topic/lỗi mà suy luận bỏ sót

Mỗi lần chạy pipeline thật rồi describe cluster lại lộ một thứ bản kê thiếu:

| Lần | Thiếu / lỗi | Nếu tắt auto-create mà bỏ qua |
|---|---|---|
| 1 | `_schemas` (store của Confluent Schema Registry) | Schema Registry không khởi động → **sập toàn bộ đường CDC Avro** |
| 2 | `__debezium-heartbeat.bankdb` (do `heartbeat.interval.ms`) | Debezium không đẩy được replication slot → **slot đứng → WAL phình vô hạn** |
| 3 | `create-topics.sh` bị **CRLF** (Python `write_text` trên Windows dịch `\n`→`\r\n`) | `set -euo pipefail\r` là option lỗi → **script chết ngay dòng đầu** |

Lần 1 và 2 cùng bản chất: topic hạ tầng do một service tự tạo, không suy ra được từ dataset — chỉ lộ khi
describe cluster đang chạy. Lần 2 nay **suy diễn** từ `TOPIC_PREFIX` (có CDC thì có heartbeat), sạch hơn
khai hằng số. Lần 3 chữa tận gốc: `cli.py write` ép `newline="\n"`, cộng `.gitattributes` `*.sh eol=lf`.

Đây đúng loại rủi ro roadmap Pha 2 cảnh báo (*"còn thiếu topic nội bộ trước khi dám tắt auto-create"*), và
là lý do **không được tắt auto-create bằng suy luận thuần**. Ba lần liên tiếp, ba thứ mắt bỏ qua — giá
trị của kỷ luật "đối chiếu với hiện thực", y như `kafka_max_block_size` ở ADR-0019.

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

## Cutover — đã hoàn thành end-to-end (2026-07-18)

Các cổng dưới đây ban đầu để dành; nay đã đi hết, đúng kỷ luật "thêm cái mới song song, kiểm chứng, rồi
mới bỏ cái cũ":

1. [x] **Nối `create-topics.sh` vào khởi động** — thêm service `kafka-init` trong compose (mount script
   LF, chạy sau kafka healthy, `--if-not-exists` nên idempotent). Kiểm chứng cô lập: `up kafka-init` →
   "Đã đảm bảo tồn tại 21 topic", exit 0.
2. [x] **Chạy pipeline đầy đủ** — deploy 7 connector bằng [connector deployer](0021-connector-deployer-idempotent.md)
   ([ADR-0021](0021-connector-deployer-idempotent.md)); Debezium snapshot sinh các topic CDC; chạy
   `create-topics.sh` tạo nốt `metrics.*` + `dlq.events`. Đối chiếu lại: **21/21 topic thật khớp bản kê,
   0 lệch/mồ côi.**
3. [x] **Tắt auto-create** — đặt `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`, recreate container kafka (config
   này không đổi động được — Kafka báo `Cannot update these configs dynamically`). 21 topic sống sót qua
   recreate (volume bền).

**Chứng minh sau khi rút lưới:**
- Config broker: `auto.create.topics.enable=false` (STATIC override thắng DEFAULT=true).
- Chạm một topic ma → `UNKNOWN_TOPIC_OR_PARTITION`, topic **không** bị tạo. Auto-create tắt dứt khoát.
- 7 connector tự reconnect sau recreate, vẫn RUNNING; snapshot (accounts=200, customers=100) chảy trọn
  Postgres → topic → ES với auto-create **tắt**. `transactions`/`transfers` rỗng ở nguồn nên 0 message —
  đúng, không phải lỗi.

Lưới cũ chỉ rút SAU khi lưới mới (bản kê + `kafka-init`) được chứng minh phủ đủ.

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
