# ADR-0045: State + thu gom rác — `metadata/` quyết định cả cái KHÔNG được tồn tại

- **Status:** Accepted
- **Date:** 2026-09-01
- **Deciders:** Phan Truong

## Bối cảnh

Sau [ADR-0044](0044-cli-apply-orchestrator.md), vòng lặp đã khép: desired state → apply có thứ tự →
verify theo lịch. Nhưng còn một lỗ hổng làm sứt chính cái nhãn "nguồn sự thật duy nhất".

`grep -rE "DELETE|prune|orphan" dataplatform/deployers/` trả về **0 dòng**. Deployer chỉ biết **cộng**,
không biết **trừ**. Hệ quả cụ thể:

- Xoá `metadata/datasets/oltp/transfers.yaml` rồi `cli write` → `es-sink-transfers.json` **vẫn nằm trên
  đĩa**, và `check` báo "khớp tuyệt đối" vì nó chỉ nhìn những file **mình sinh ra**.
- Chạy `connectors apply` → `es-sink-transfers` **vẫn sống trên Kafka Connect mãi mãi**, vẫn tiêu thụ
  topic, vẫn ghi vào Elasticsearch. Không cổng nào phát hiện: `check` chỉ nhìn đĩa, verifier chỉ đối
  chiếu những thứ **có** trong contract.

Nói chặt chẽ: `metadata/` là nguồn sự thật **của tập artifact**, chưa phải **của trạng thái hệ thống**.
Nó quyết định được cái gì *phải* tồn tại, nhưng không quyết định được cái gì *không được* tồn tại.

## Quyết định

Chia tài nguyên làm **hai lớp theo khả năng hồi phục**, và đối xử khác nhau:

| Lớp | Tài nguyên | Cách xử lý |
|---|---|---|
| **Tạo lại được từ metadata** | file artifact sinh, config connector | **tự động xoá** |
| **Mang dữ liệu** | topic Kafka, bảng ClickHouse, đường dẫn S3 | **chỉ báo cáo, không bao giờ tự xoá** |

Đây là ranh giới trung tâm của ADR này. Xoá một connector chỉ bỏ cấu hình — topic và bản ghi còn
nguyên, và `apply` lại là có ngay. `DROP TABLE` thì không có nút undo. **Tự động hoá + không hồi phục
được là kết hợp tệ nhất**, nên lớp thứ hai dừng ở mức báo cáo kèm lệnh thủ công.

`tests/test_orphan_gc.py` khoá ranh giới này lại: đường GC không được chứa `DROP TABLE`, `--delete`,
`delete_topics`. Ai đó sau này thêm vào thì test đỏ.

### File thừa — không cần state

`OWNED_GLOBS` khai những thư mục control plane **sở hữu trọn vẹn**:

```
kafka-connect/es-sinks/*.json
kafka-connect/s3-sinks/*.json
trino/etc/catalog/*.properties
```

Bất cứ file nào khớp glob mà không nằm trong `_collect()` đều là rác. Không cần state vì câu hỏi
"file này có được sinh không" trả lời được ngay từ metadata. `check` báo `[THỪA]` và **exit 1**;
`write` xoá.

Chỉ khai được glob có hình dạng "N dataset → N file". Artifact đơn lẻ (`debezium/postgres-connector.json`,
`kafka/topics.json`…) không bao giờ thành thừa vì chúng luôn được sinh, chỉ nội dung thay đổi.

### Connector thừa — cần state

`.platform-state.json` ghi những connector **chính ta đã tạo**. `apply` tính
`state − desired` = thừa, rồi `DELETE`.

**Vì sao dùng state chứ không so thẳng với connector đang sống trên Connect:** so thẳng sẽ xoá cả
connector người khác tạo tay để thử nghiệm. Chỉ xoá thứ mình tạo ra là mô hình của Terraform, và là lựa
chọn an toàn hơn. Đánh đổi: lần `apply` đầu tiên trên state rỗng không phát hiện được gì — phải chạy
một lần để ghi state trước. Chấp nhận được, và giống hệt Terraform khi `import` chưa chạy.

State **không commit** (`.gitignore`): mỗi môi trường một bản. Commit vào sẽ làm máy khác tưởng mình đã
tạo những thứ chưa hề tạo — và lần `apply` sau sẽ **xoá nhầm**.

Xoá chạy **trước** khi áp: nếu một dataset bị đổi tên, ta muốn bỏ connector cũ rồi mới tạo cái mới,
thay vì để hai cái cùng đọc một topic một lúc.

### Bảng ClickHouse thừa — chỉ báo

`verifiers/clickhouse_schema` nay so danh sách bảng trong `metrics` với contract và **báo** bảng không
còn được khai, kèm lệnh `DROP TABLE` để người tự chạy. Là **cảnh báo, không phải lỗi** — không đổi mã
thoát, vì một bảng thừa không làm pipeline sai, nó chỉ tốn chỗ và gây nhầm lẫn.

Gom theo tên gốc trước khi so (`<tên>`, `<tên>_kafka`, `<tên>_mv` là một metric), và bỏ qua bảng do hệ
thống tạo: `schema_migrations`, `dlq_events`, `notification_events`.

**Topic Kafka thừa: đã có sẵn** — `verifiers/kafka_topics.compare()` báo chúng dưới dạng warning từ
[ADR-0040](0040-tighten-topic-layer.md). Không cần làm gì thêm.

## Kiểm chứng live

Chạy đúng kịch bản đã nêu ở phần Bối cảnh, trên stack thật:

```
xoá metadata/datasets/oltp/transfers.yaml
  cli check   -> [THỪA] kafka-connect/es-sinks/es-sink-transfers.json   exit 1
  cli write   -> đã XOÁ es-sink-transfers.json
  connectors apply -> [OK ] DELETE es-sink-transfers (HTTP 204)
  GET /connectors  -> 6 connector, es-sink-transfers ĐÃ BIẾN MẤT

khôi phục transfers.yaml
  cli write   -> 19 artifact
  connectors apply -> [OK ] CREATE es-sink-transfers (HTTP 201), RUNNING
  GET /connectors  -> 7 connector
```

Bảng thừa: tạo `metrics.bang_rac_thu_nghiem` → verifier **báo** `1 bảng thừa (chỉ báo)`, exit **0**, và
kiểm lại thì **bảng còn nguyên**. Đúng ranh giới: báo chứ không xoá.

## Hệ quả

- Dễ hơn: xoá dataset khỏi `metadata/` nay dọn sạch được cả file lẫn connector. `metadata/` thật sự là
  nguồn sự thật cho **cả hai** vế.
- Dễ hơn: `check` nay bắt được file rác — trước đây nó mù hoàn toàn với loại này.
- Khó hơn: có thêm một file state phải hiểu. Mất state thì lần `apply` sau không GC được cho tới khi
  chạy lại một lần — không nguy hiểm, chỉ là rác sống lâu hơn.
- Khó hơn: thêm một generator sinh nhiều file phải nhớ khai vào `OWNED_GLOBS`, nếu không vùng đó không
  được GC. `test_orphan_gc.py` bắt được glob trỏ sai, nhưng **không** bắt được glob còn thiếu.

## Phương án đã cân nhắc

- **So thẳng engine với desired, không dùng state** — loại: sẽ xoá connector do người khác tạo tay.
- **Xoá luôn cả topic và bảng cho triệt để** — loại: mất dữ liệu không hồi phục được, và một lần chạy
  nhầm `--as-of` hay nhầm nhánh git là mất sạch. Ranh giới "tạo lại được / mang dữ liệu" đáng giá hơn
  sự triệt để.
- **Chỉ báo cáo, không xoá gì cả** — loại: đó chính là hiện trạng, và nó để rác sống mãi. Với thứ tạo
  lại từ metadata là có ngay thì báo cáo rồi bắt người gõ tay là công việc vô ích.
- **Commit state vào git** — loại: state là per-environment; commit vào thì máy khác `apply` sẽ xoá
  nhầm những thứ nó chưa hề tạo.
