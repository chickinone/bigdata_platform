# ADR-0047: Verifier sức khoẻ Kafka Connect — task, không phải connector

- **Status:** Accepted — verify live cả hai chiều
- **Date:** 2026-09-02
- **Deciders:** Phan Truong

## Bối cảnh

Trong lúc chạy test tải, `postgres-source-connector` báo:

```
connector: RUNNING     tasks: ['FAILED']
```

CDC đã chết **hơn một ngày** ở trạng thái đó. Offset `bankdb.public.transactions` đứng im ở 150.968
trong khi generator vẫn ghi vào Postgres. Không gì báo động.

Nguyên nhân gốc: Schema Registry restart (lúc Docker khởi động lại); trong lúc nó chết, Debezium cần
đăng ký schema cho topic heartbeat → `Connection refused` → vượt ngưỡng lỗi → **task FAILED vĩnh viễn**.
**Kafka Connect không tự khởi động lại task đã chết.**

Ngay sau đó, dưới tải ~650 msg/s, **4/5 ES sink** chết theo cùng kiểu — `Bulk request failed` →
`Unable to parse response body` → `NullPointerException`. Elasticsearch vẫn sống (`yellow`, không OOM,
RAM tổng dùng chưa tới một nửa). Tức là ES dội ngược, và **bộ xử lý lỗi của connector NPE rồi chết hẳn**
thay vì lùi lại thử lại.

Cả bảy connector trong cả hai sự cố đều báo `connector: RUNNING`.

## Điểm cần nói rõ: deployer KHÔNG sai

Nghi ngờ đầu tiên là `connectors apply` chỉ nhìn `connector.state`. Kiểm lại thì **không phải** —
`_wait_running` đã xét đủ:

```python
if cstate == "FAILED" or "FAILED" in tstates:
    result[name] = "FAILED"
```

Lúc `apply` chạy, task thật sự đang RUNNING. Nó chết **sau đó**. Lỗ hổng không nằm ở lúc triển khai mà ở
**khoảng giữa hai lần apply**: không có gì canh sức khoẻ connector khi không ai gõ lệnh.

Ta đã có verifier cho topic, schema Avro, publication, ClickHouse, Trino, OpenMetadata — **không có cho
Kafka Connect**, đúng chặng mà mọi dữ liệu phải đi qua.

## Quyết định

Thêm `verifiers/connect_health.py` vào `VERIFIERS` (nay 9 verifier). Với mỗi connector khai trong
metadata, kiểm ba điều theo thứ tự tinh vi tăng dần:

| # | Kiểm | Bắt được gì |
|---|---|---|
| 1 | connector có tồn tại | ai đó xoá tay, hoặc `apply` chưa chạy |
| 2 | **mọi task đều RUNNING** | **đúng lỗ hổng của ADR này** |
| 3 | task không rỗng | connector RUNNING với 0 task — trông sạch, chạy rỗng |

`PAUSED` **cũng bị coi là lệch**: connector tạm dừng trông "không lỗi" nhưng không tiêu thụ gì — đúng
kiểu im lặng verifier này sinh ra để bắt.

### `_dong_goc()` — lấy `Caused by` sâu nhất

Kafka Connect bọc lỗi nhiều tầng; dòng đầu luôn là `Tolerance exceeded in error handler`, chẳng nói gì.
Nguyên nhân thật nằm ở `Caused by` **cuối cùng** — `Connection refused`, `Bulk request failed`. Verifier
in đúng dòng đó, nên đọc log là biết ngay phải sửa gì.

### Mã thoát, theo đúng lệ đã dùng cả loạt ADR

| Tình huống | Mã |
|---|---|
| Connect **không nối được** | **3** — chưa kết luận được gì |
| thiếu connector / task không RUNNING / 0 task | **1** |
| connector lạ trên Connect | 0 + chú ý (GC của [ADR-0045](0045-orphan-gc-state.md) mới là chỗ dọn) |

Gộp mã 3 vào mã 1 sẽ biến "stack chưa bật" thành báo động giả, mà báo động giả lặp lại thì cả cổng bị bỏ
qua — cùng lý do đã chọn `--no-git` cho gitleaks ([ADR-0041](0041-harden-perimeter.md)).

Verifier dùng thẳng `dep.desired_connectors()` và `dep._req()`, không giữ bản sao danh sách connector.
Và **chỉ đọc**: có test cấm mọi `PUT`/`POST`/`DELETE`.

## Kiểm chứng live

```
7 connector, mọi task RUNNING          -> KẾT QUẢ: 0 lệch          exit 0
PUT /connectors/es-sink-customers/pause
  -> [LỖI ] es-sink-customers  connector=PAUSED  tasks=['PAUSED']  exit 1
PUT .../resume
  -> KẾT QUẢ: 0 lệch, 7 connector và mọi task đều RUNNING          exit 0
cli verify -> 9/9 verifier DAT
```

Chứng minh **cả hai chiều**: không báo sai khi khoẻ, và có báo đúng khi hỏng. Chỉ chiều thứ nhất thì
chưa đủ để tin.

## Hệ quả

- Dễ hơn: task chết nay bị phát hiện trong vòng một giờ (`cli verify` chạy theo lịch), thay vì khi ai đó
  tình cờ nhận ra dữ liệu không tới.
- Dễ hơn: thông điệp kèm sẵn lệnh khôi phục (`restart?includeTasks=true&onlyFailed=true`).
- **Vẫn chưa làm:** không tự khởi động lại task. Cố ý — tự restart che mất nguyên nhân gốc, và một task
  chết đi chết lại cần người nhìn chứ không cần vòng lặp restart. Verifier báo, người quyết định.
- Còn hở: verifier chỉ chạy mỗi giờ. Một task chết ngay sau lần chạy thì tối đa 60 phút sau mới lộ.

## Phương án đã cân nhắc

- **Tự động restart task FAILED** — loại: che nguyên nhân gốc. Nếu ES dội ngược thì restart chỉ làm nó
  chết lại sau vài phút, và ta mất tín hiệu là hệ thống đang quá tải.
- **Dựa vào healthcheck container của Kafka Connect** — loại: container Connect **vẫn khoẻ** trong cả hai
  sự cố. Tiến trình sống không nói gì về việc task bên trong có chạy không.
- **Đọc lag consumer thay vì task state** — loại: lag cao có thể chỉ là đang bận. Và khi task chết hẳn,
  lag **đứng im** chứ không tăng — nhìn lag sẽ thấy "ổn định", tưởng là tốt.
