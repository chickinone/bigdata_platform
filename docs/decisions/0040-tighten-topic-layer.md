# ADR-0040: Siết chặt chặng topic — topic duy nhất, một nguồn RF, config trong contract, verifier broker

- **Status:** Accepted — 5 thay đổi, `check` 19/19 không đổi (refactor byte-identical); verifier broker chưa chạy live
- **Date:** 2026-07-30
- **Deciders:** Phan Trường

## Bối cảnh

[ADR-0020](0020-generate-kafka-topic-manifest.md) sinh bản kê topic từ registry và cho phép tắt
`auto.create.topics`. Rà lại chặng đó (generator `topic_manifest.py` + `dlq.py`) lộ ra 5 chỗ chưa chặt —
không cái nào đang gây sự cố, nhưng cả 5 đều là **cùng một loại nợ**: một sự thật được khai hoặc suy ở hai
nơi, hoặc không được đối chiếu với hiện thực.

1. **Không ai chặn hai contract cùng `source.topic`.** `_check_unique_urns` chặn URN trùng; JSON Schema chỉ
   nhìn từng file. Hai URN khác nhau cùng một topic là hợp lệ với cả hai lớp kiểm.
2. **`partitions` không khai được trong contract.** `DEFAULT_PARTITIONS = 1` cứng cho mọi dataset — muốn
   `transactions` nhiều partition hơn phải sửa generator, mà sửa generator thì tăng cho *tất cả*.
3. **Retention không nằm ở đâu cả.** `configs: {}` cho mọi topic dữ liệu; bản kê không trả lời được "topic
   này giữ dữ liệu bao lâu".
4. **Predicate "dataset nào có sink X" bị nhân bản.** `dlq.py` chép lại biểu thức lọc của
   `s3_sink._members()` và `es_sink.targets()` thay vì gọi chúng.
5. **RF khai hai lần**: `topic_manifest.REPLICATION_FACTOR = 1` và `dlq.DLQ_REPLICATION_FACTOR = "1"`, ở hai
   file, cả hai kèm comment "lên multi-broker thì đổi".

Chỗ 4 đáng nói nhất vì nó nằm trong **chính generator được viết để đóng sprawl #12**: nếu ai đó nới luật
"dataset nào vào Bronze" ở `s3_sink` mà quên `dlq.py`, thì `original_topics` thiếu topic và dlq-processor mất
khả năng truy nguyên nguồn lỗi — đúng chế độ hỏng mà `dlq.py` sinh ra để chống.

## Quyết định

### 1. `_check_unique_topics()` trong registry

Chiều thứ hai của `_check_unique_urns`, cùng vị trí, cùng cách ném `ContractError`. Topic là định danh
**thật** của dòng dữ liệu trên Kafka; trùng topic nghĩa là bản kê có hai dòng cùng `name` (tự nói dối), hai
ES sink cùng đọc một topic rồi ghi đè `_id` của nhau, hai topic DLQ cho cùng một nguồn lỗi.

### 2. `source.partitions` + `source.topic_configs` là trường contract

Mức song song và chính sách giữ dữ liệu là thuộc tính của **dòng dữ liệu**, không phải hằng số của
generator. Cả hai optional; thiếu thì dùng mặc định platform, nên hiện trạng không đổi (`check` 19/19).

`topic_configs` gộp **sau** cờ `compact` — contract thắng, vì nó là khai báo có chủ ý của người sở hữu
dataset.

**Cố ý không dùng ngay:** `transactions` (150–800 RPS) là ứng viên rõ ràng cho nhiều partition, nhưng đổi
partition của topic nguồn làm thay đổi phân bố `key_by` của fraud detector. Đó là thay đổi hành vi runtime,
phải là quyết định riêng có đối chiếu — không lẫn vào một PR refactor. Ràng buộc này ghi vào `description`
của schema để lần sau đọc là thấy.

### 3. RF về `connections/kafka.yaml`

RF là thuộc tính của **cluster**, nên nó thuộc connection, không thuộc generator. `dlq_config()` nhận RF qua
tham số; `topic_manifest` đọc `endpoint(conns, "kafka", "replication_factor")`. Một nơi khai, hai nhóm topic
(dữ liệu + DLQ) không thể lệch.

### 4. `dlq.py` gọi luật của generator sink

`es_sink.members()` và `s3_sink.members()` thành public. Luật "dataset nào có sink X" thuộc về generator của
sink X; ai cần thì gọi. Cùng nguyên tắc `postgres_publication` gọi `debezium.cdc_datasets()`
([ADR-0018](0018-generate-debezium-and-publication.md)) và `clickhouse_schema` gọi `clickhouse_ddl._ch_type`.

### 5. `verifiers/kafka_topics.py`

Đối chiếu bản kê với `kafka-topics --describe`. Cùng họ và cùng lý do với
[ADR-0039](0039-verify-publication-vs-contract.md): `cli check` chỉ chứng minh bản kê khớp metadata, nó
không biết broker có gì. Mà `create-topics.sh` chỉ chạy khi dựng stack (`kafka-init`, `restart: "no"`).

Ba mức nghiêm trọng khác nhau:

| Lệch | Mức | Vì sao |
|---|---|---|
| THIẾU (bản kê có, broker không) | error | pipeline đứt ở topic đó |
| LỆCH partitions / RF | error | `--if-not-exists` **không** sửa được topic đã tồn tại |
| THỪA (broker có, bản kê không) | warning | topic rác thời auto-create, hoặc dataset đã xoá |

Chỉ so những **khoá config mà bản kê khai** — giống cách deployer connector chỉ xét khoá trong desired. Bản
kê cố ý không khai giá trị mặc định broker (ADR-0020), nên so toàn bộ sẽ báo lệch giả.

`__consumer_offsets`/`__transaction_state` vào whitelist: broker tự quản, control plane không khai và không
xoá được — không whitelist thì chúng báo THỪA vĩnh viễn và verifier thành thứ bị bỏ qua.

Parser tách thành `parse_describe(stdout)` thuần để test được mà không cần broker.

## Kiểm chứng (đo thật)

**Offline (không cần stack):**

- `parse_describe`: đúng với `Configs` rỗng, nhiều config, và dòng chi tiết partition thụt lề.
- `compare`: bắt THIẾU; bắt lệch partitions (3 vs 25), RF (2 vs 1), config (`delete` vs `compact`);
  `__consumer_offsets` **không** bị báo THỪA.
- `_check_unique_topics`: hai dataset cùng `same.topic` → `ContractError` chỉ ra cả hai file.
- Contract-driven: khai `partitions: 6` + `topic_configs: {retention.ms}` trong bộ nhớ → bản kê đổi theo;
  đổi RF một chỗ → cả `bankdb.public.transactions` lẫn `dlq.es-sink-transactions` cùng đổi.
- `cli check`: **19/19 khớp** — refactor 1–4 không đổi một byte artifact nào.
- `grep`: không còn tham chiếu `DLQ_REPLICATION_FACTOR` hay `_members`.

**Chưa chạy:** `verifiers.kafka_topics` đối chiếu broker thật (Docker Desktop tắt lúc làm). Parser đã test
bằng output mẫu nhưng **chưa xác nhận với đúng image `confluentinc/cp-kafka:7.7.1`** — format `--describe`
có thể khác ở thứ tự field. Đây là rủi ro còn lại, phải chạy live trước khi tin.

## Hệ quả

**Dễ hơn:** topic trùng bị chặn ở generation. RF đổi một dòng. Contract biểu đạt được ý định vận hành
(partition, retention) mà trước đây chỉ generator biết. Lệch broker phát hiện bằng một lệnh.

**Khó hơn / phải chấp nhận:**

- `dlq_config()` đổi signature (thêm `replication_factor`) — mọi caller phải có `conns`. Hiện cả hai caller
  đều có; generator sink mới cũng sẽ có.
- `members()` thành public API giữa các generator — ràng buộc nhẹ giữa chúng, nhưng đó là ràng buộc **có
  chủ ý** thay thế cho bản copy ngầm.
- Verifier cần Kafka sống → nhóm verifier runtime, không vào CI tĩnh.
- `partitions`/`topic_configs` khai được nhưng **chưa dataset nào dùng**. Khả năng không dùng là nợ nhẹ; đổi
  lại là không nhét thay đổi runtime vào PR refactor.

## Phương án đã cân nhắc

- **Kiểm topic trùng bằng JSON Schema.** Không thể: schema chỉ thấy một file tại một thời điểm. Cùng lý do
  `_check_unique_urns` phải là code.
- **Đặt RF trong `.env` thay vì connection.** Loại: `.env` là secret/endpoint runtime, còn RF là **thuộc
  tính topology** mà generator phải biết lúc render. Connection registry đúng chỗ hơn (ADR-0025/0029).
- **Cho `dlq.py` nhận danh sách connector từ tham số** thay vì gọi generator sink. Loại: đẩy việc ghép về
  `cli._collect()`, làm `dlq.targets()` không tự đứng được và khó test hơn.
- **Verifier tự tạo topic thiếu.** Loại: verifier chỉ đọc (ADR-0039). Tạo topic là việc của
  `create-topics.sh` — đã idempotent, đã sinh từ cùng bản kê.
- **So toàn bộ config broker.** Loại: báo lệch giả với mọi override broker tự đặt (vd `segment.bytes`), và
  verifier bị bỏ qua sau vài lần báo sai.
