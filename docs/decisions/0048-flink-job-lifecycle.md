# ADR-0048: Vòng đời job Flink — apply là thay thế, và có verifier cho crash loop

- **Status:** Accepted — verify live cả hai chiều
- **Date:** 2026-09-02
- **Deciders:** Phan Truong

## Bối cảnh

Một lần chạy end-to-end trên stack sạch, với generator bơm không giới hạn, làm lộ bốn lỗi. Ba trong số
đó cùng một hình dạng đã gặp nhiều lần trong loạt ADR gần đây: **tín hiệu nói dối**.

## 1. Bronze trống làm `cli apply` đỏ

Trên stack vừa dựng, `spark_batch` gãy vì `s3a://data-lake-bronze/topics/bankdb.public.customers/`
chưa tồn tại.

Nguyên nhân không phải hỏng: S3 sink chỉ ghi khi đủ `flush.size=1000` bản ghi, **hoặc** tới
`rotate.interval.ms` (5 phút, đo theo *thời gian bản ghi*), **hoặc** `rotate.schedule.interval.ms`
(10 phút, đồng hồ thật).

`customers` chỉ có **100 dòng**, seed một lần, generator không tạo khách hàng mới. Nên nó **không bao
giờ** đạt `flush.size`, và `rotate.interval.ms` cũng không nổ vì không có bản ghi mới để đẩy đồng hồ
record-time. Chỉ còn rotation theo đồng hồ thật — **10 phút sau khi Kafka Connect khởi động**.

Nghịch lý đáng nhớ: **tầng batch bị chặn bởi bảng NHỎ NHẤT, không phải bảng lớn nhất.** Đo thực tế:
`transactions` 52 file, `accounts` 48, `transfers` 25, `customers` **0**.

**Vá:** `medallion_runner` bắt ba dạng thông điệp path trống, in `CHUA SAN SANG` và thoát **mã 4**.
`spark_batch` hiểu mã 4 là "chưa sẵn sàng", dừng chuỗi và trả **0**.

Vì sao mã riêng thay vì nuốt lỗi: gộp vào mã lỗi thì `cli apply` **đỏ mỗi lần dựng lạnh** — cổng luôn
đỏ là cổng bị bỏ qua ([ADR-0041](0041-harden-perimeter.md)). Nuốt thành công thì che mất lỗi thật. Ranh
giới: nếu S3 sink **hỏng** (không phải chậm) thì `connect_health` mới là chỗ báo, không phải bước spark.

## 2. `on_timer` không trả iterable — crash loop 1.104 lần

```python
def on_timer(self, timestamp, ctx):
    ...        # không có `yield` nào, không `return` gì
```

PyFlink gọi `yield from on_timer(...)` (`input_handler.py:111`). Hàm không có `yield` nên Python coi nó
là hàm thường và trả `None` → `yield from None` → `TypeError: 'NoneType' object is not iterable`.

`process_element` không dính vì nó **có** `yield` nên đã là generator.

**Vì sao ẩn được lâu đến thế:** job chạy bình thường cho tới khi timer ĐẦU TIÊN nổ (cần đủ giao dịch
`failed` dồn trong cửa sổ 5 phút), rồi crash → Flink restart → chạy tiếp → crash. Giữa hai lần chết,
`flink list` **vẫn hiện RUNNING**. Attempt number đã tới `_245`, log có **1.104 lỗi giống hệt nhau**.

**Vá:** `return []` ở cuối, và chặn thêm `self.failed_history.get() or []` — `ListState.get()` trả
`None` khi state rỗng, `list(None)` cũng nổ cùng kiểu.

## 3. `flink_metrics apply` cộng thêm chứ không thay thế

`cmd_apply` submit thẳng, không huỷ gì. Chạy `apply` lần hai tạo **bản metric_runner thứ hai**: hai job
cùng đọc một topic và cùng ghi vào **cùng sink ClickHouse** → metric nhân đôi, không lỗi nào. Quan sát
thật: `flink list` hiện hai job `insert-into_...` ở 04:41:44 và 05:05:00.

**Vá:** huỷ job của lần apply trước **rồi mới** submit bản mới, dùng `.platform-state.json` — chỉ huỷ
job **mình đã tạo**, cùng lý do đã chọn state cho GC connector ([ADR-0045](0045-orphan-gc-state.md)):
so thẳng với danh sách đang chạy sẽ huỷ nhầm job người khác submit tay.

## 4. Không có verifier nào canh Flink — đây là gốc rễ

Lỗi #2 ẩn được 1.104 lần **không phải vì nó khó thấy**, mà vì **không có gì nhìn**. Ta đã có verifier
cho postgres schema/publication, kafka topics, Connect task, Avro, ClickHouse, Trino, OpenMetadata,
quality — nhưng **không có cho Flink**, đúng chặng xử lý dòng.

**Vá:** `verifiers/flink_jobs.py`, đưa vào `VERIFIERS` (nay 10 verifier).

Điểm thiết kế quan trọng nhất: **không chỉ đọc state tức thời.**

| Kiểm | Bắt được gì |
|---|---|
| job còn trên cluster | jobmanager restart (job không HA), bị huỷ tay |
| state == RUNNING | job chết hẳn |
| **`exceptionHistory` rỗng** | **crash loop — job RUNNING nhưng đã chết N lần** |

Tầng ba là mấu chốt. `flink list` chỉ hiện trạng thái **tức thời**, nên job chết-rồi-restart liên tục
vẫn hiện RUNNING mỗi lần ta nhìn. Lịch sử exception thì **cộng dồn** — nó không nói dối.

Verifier cũng cảnh báo job đang RUNNING mà **không có trong state** — chính là triệu chứng của lỗi #3.

Mã thoát theo đúng lệ: Flink không nối được → **3** (chưa kết luận); job mất/không RUNNING/có lịch sử
hỏng → **1**; job lạ hoặc đang khởi động (`CREATED`/`INITIALIZING`/`RESTARTING`) → **0** + chú ý.

## Kiểm chứng live

```
verifier, đường xanh : 2 job RUNNING, 0 lần hỏng                    exit 0
huỷ tay fraud_runner : [LỖI] fraud_runner CANCELED (mong đợi RUNNING) exit 1
flink_metrics apply  : [OK] huỷ job cũ metric_runner: f8286fd6
                       [OK] metric_runner + fraud_runner submit mới
sau đó               : 2 job RUNNING, KHÔNG còn bản trùng           exit 0
cli verify           : 10/10 DAT
```

Bản vá #2 chứng minh bằng **số lỗi đóng băng**: mốc 1.104 lúc 05:05:49, vẫn 1.104 lúc 05:11:55 — không
tăng một lỗi nào qua trọn hơn một chu kỳ cửa sổ 5 phút, trong khi trước vá nó chết mỗi ~30 giây. Và
detector **làm việc thật**, không chỉ "không chết": 3.744 alert, 1.556 `notification_events`.

## Hệ quả

- Dễ hơn: `cli apply` chạy được trên stack vừa dựng mà không đỏ giả.
- Dễ hơn: crash loop Flink bị phát hiện trong vòng một giờ (`cli verify` chạy theo lịch).
- Dễ hơn: `flink_metrics apply` nay idempotent — chạy nhiều lần không sinh bản trùng.
- **Vẫn còn:** job Flink **không HA**. Jobmanager restart là mất job; verifier báo `[MẤT]` nhưng không
  tự submit lại. Cố ý — tự động submit lại che mất việc jobmanager đang chết đi chết lại.
- Còn hở: verifier chạy mỗi giờ, nên một job chết ngay sau lần chạy thì tối đa 60 phút sau mới lộ.

## Phương án đã cân nhắc

- **Cho `on_timer` là generator bằng `yield` giả** — loại: `return []` nói rõ ý định hơn ("không phát gì")
  và không tạo generator rỗng gây nhầm.
- **Huỷ TẤT CẢ job đang chạy trước khi apply** — loại: sẽ huỷ nhầm job người khác submit tay để thử.
- **Tự động submit lại job bị mất** — loại: che mất nguyên nhân gốc. Job chết đi chết lại cần người
  nhìn, không cần vòng lặp submit.
- **Chỉ kiểm `state` của job, bỏ qua lịch sử exception** — loại: đó chính là cách lỗi #2 trốn được
  1.104 lần.
