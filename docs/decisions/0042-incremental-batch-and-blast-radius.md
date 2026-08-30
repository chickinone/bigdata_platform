# ADR-0042: Chặn bán kính sát thương — trần WAL, tiêu chí đậu dùng chung, batch chạy theo cửa sổ

- **Status:** Accepted — verify live xong trên 551.769 dòng thật: parity với full refresh, idempotent qua hai lần chạy, 33 partition ngoài cửa sổ nguyên vẹn. Trần WAL áp trên Postgres đang chạy (`safe_wal_size≈2.15GB`, 7/7 connector `RUNNING` sau restart). 62 test xanh, `check` 19/19.
- **Date:** 2026-08-14
- **Deciders:** Phan Trường

## Bối cảnh

Ba khoảng trống còn lại sau khi lộ trình metadata-driven đóng ([ADR-0037](0037-cutover-complete-single-source.md)),
đều nằm ngoài phạm vi metadata nên chưa ai đụng tới. Rà lại thì chúng **không cùng loại nguy hiểm**, và đó
là điều quyết định thứ tự xử lý:

1. **Single-node** — nguy hiểm không nằm ở "chậm" mà ở chỗ nó giết hệ thống *khác*.
2. **Full refresh** — có ngày hết hạn tính được bằng máy tính.
3. **Airflow chưa chạy task e2e** — làm hai cái trên trở nên vô hình.

### 1. Replication slot không có trần

`docker-compose.yml` đặt `wal_keep_size=512MB` — nhưng đó là mức *tối thiểu* giữ cho streaming replica, nó
**không** giới hạn WAL mà một replication slot níu lại. Cái làm việc đó là `max_slot_wal_keep_size` (PG13+),
mặc định `-1` = không giới hạn.

Chuỗi hậu quả:

```
Kafka broker chết (RF=1, không có replica để promote)
  -> Debezium không produce được -> không commit offset
  -> slot Postgres không advance confirmed_flush_lsn
  -> Postgres GIỮ WAL -> pg_wal phình -> đầy đĩa
  -> Postgres ngừng nhận write
```

Database OLTP — thứ quan trọng nhất trong hệ thống — chết vì Kafka analytics chết. Đây là **đảo ngược bán
kính sát thương**, và nó không liên quan gì tới quy mô dữ liệu.

### 2. Tiêu chí đậu bị mất khi đi qua DAG

`deployers/spark_batch._submit` đòi `returncode == 0` **và** có dòng `WROTE`. `generators/airflow_dag` thì
dựng `BashOperator(bash_command=" ".join(submit_argv(spec)))` — mà BashOperator chỉ phán theo exit code.

Nên `submit_argv` được chia sẻ đúng như thiết kế, còn **tiêu chí "thế nào là chạy xong" thì không**. Hệ quả:
spark-submit exit 0 mà job không ghi gì (quyền S3 sai, path rỗng, cluster deploy-mode trả 0 ngay khi submit)
thì Airflow **báo xanh**. Một orchestrator báo xanh sai tệ hơn không có orchestrator, vì từ đó ta tin nó.

Đây đúng là loại sprawl mà cả dự án đang diệt — một sự thật ("job đậu khi nào") khai ở hai nơi — còn sót
trong chính control plane, giống hệt phát hiện về `pipelines/` ở [ADR-0041](0041-harden-perimeter.md).

### 3. Toàn bộ medallion là full refresh

Không chỉ Silver như `BDP-current-state.md` mục 4.2 #13 ghi — **cả 5 spec** đều `mode: overwrite` đọc lại
toàn bộ lịch sử.

Với ~150 RPS (~13 triệu bản ghi/ngày): ngày N, Silver đọc `N × 13 triệu` dòng để thêm 13 triệu.

- Chi phí mỗi lần chạy: **O(t)**
- Chi phí tích luỹ: **O(t²)** — sau 1 năm tốn ~180 lần I/O so với incremental

Nếu ngày 1 chạy 2 phút thì ngày 365 chạy ~12 tiếng, ngày 730 chạm 24 tiếng: **lịch `@daily` không bao giờ
đuổi kịp nữa**. Và không có lối thoát bằng scale ngang vì chỉ có 1 Spark worker.

Tệ hơn cả chậm: `mode("overwrite")` ở chế độ static **xoá path rồi mới ghi**. Có một cửa sổ Silver rỗng;
job chết ở đó thì Gold chạy ngay sau đọc rỗng và ghi đè Gold bằng rỗng. Job càng lâu (do O(t)) cửa sổ càng
rộng — hai nợ nhân nhau. Trớ trêu là đường Iceberg trong cùng file runner đã ghi nguyên tử sẵn
(`writeTo().createOrReplace()` đổi snapshot), chỉ đường parquet là không.

## Quyết định

Ba sửa chữa, độc lập nhau, xếp theo mức thiệt hại thật:

**1. Đặt trần WAL cho slot.** `max_slot_wal_keep_size=2GB` trong `docker-compose.yml`. Vượt trần thì slot bị
invalidate — **mất liên tục CDC** (phải snapshot lại), nhưng **database sống**. Đây là đánh đổi có chủ đích:
trước đây ta ngầm chọn vế ngược lại mà không biết mình đang chọn.

**2. Một nguồn cho tiêu chí đậu.** Hằng `SUCCESS_MARKER` trong `spark_batch.py`, dùng cho cả `_submit`
(Python) lẫn `bash_command()` (shell) mà generator Airflow gọi. Lệnh sinh ra:

```
<spark-submit> 2>&1 | tee /tmp/medallion_<job>.$$.log; grep -q '^WROTE ' /tmp/...
```

`grep` đứng **cuối** nên nó là trọng tài, không phải spark-submit. Dùng `tee` + file chứ không `grep -q`
thẳng trên pipe vì `-q` thoát sớm sẽ đóng pipe và làm spark-submit dính SIGPIPE.

**3. Batch chạy theo cửa sổ ngày.** Khối `incremental` mới trong `pipeline.schema.json`, khai trong contract:

```yaml
incremental:
  lookback_days: 3
  date_columns: [year, month, day]
  windowed_inputs: [bronze_transactions]
  input_margin_days: 1
```

Runner cắt input theo partition, tính lại **trọn vẹn** các partition trong cửa sổ, rồi `overwrite` ở chế độ
**dynamic** — thay đúng những partition đó, không đụng phần còn lại.

### Ba bất biến, và vì sao từng cái sống còn

Ghi đè động thay **thẳng cả partition**. Nên một partition chỉ tính được *một phần* sẽ xoá mất phần còn lại.
`incremental_problems()` chặn trước khi ghi:

| Bất biến | Vi phạm thì sao |
|---|---|
| `partition_by` không rỗng | Không có gì khoanh vùng -> ghi đè **tất cả** |
| `date_columns` là tiền tố của `partition_by` | Lọc theo cột này mà khoanh theo cột kia -> xoá nhầm |
| `mode == overwrite` | `append` + retry của Airflow = **nhân bản** dữ liệu |

Và bất biến thứ tư nằm trong luồng chạy: sau khi chạy SQL, kết quả bị **lọc lại** theo `[start, end]`.
`input_margin_days` kéo dữ liệu thừa *vào*, dòng lọc này đẩy *ra* — nhờ vậy không partition nào bị ghi khi
mới tính nửa vời. Đây là chỗ dễ sai nhất của toàn bộ thiết kế.

### Vì sao margin tồn tại

Bronze partition theo **thời điểm CDC bắt được**; Silver partition theo **`posted_at`**. Hai mốc đo khác
nhau, nên đọc dư 1 ngày cho chắc. Chặng Silver -> Gold thì `margin = 0` vì partition Silver *đã là* ngày
nghiệp vụ — cùng thang đo với output.

Giả định đứng sau: `posted_at = NOW()` lúc INSERT (`generator/generators.py`), nên `posted_at <= thời điểm
capture` luôn đúng. Nếu sau này có giao dịch **đề ngày tương lai** xa hơn lookback, nó sẽ lọt lưới cho tới
lần `--full-refresh`.

### Hai job cố ý KHÔNG incremental

- **`gold_customer_lifetime_metrics`** — "trọn đời" là gộp trên toàn bộ lịch sử mỗi khách.
  `count(DISTINCT account_id)` không cộng dồn được từ các mảnh; ép cửa sổ vào sẽ cho ra **số sai**, không
  phải số cũ. Vẫn O(t) nhưng đọc Silver chứ không đọc Bronze. Muốn sửa thì hướng là Iceberg MERGE tích luỹ.
- **`iceberg_silver_enriched`** — `createOrReplace()` đã nguyên tử sẵn; vấn đề nguy hiểm nhất của overwrite
  không tồn tại ở đây.

Lý do ghi thẳng vào chính hai file spec, và có test khoá lại (`test_hai_job_khong_partition_aligned_van_full_refresh`)
để lần sau không ai bật incremental cho chúng mà quên mất vì sao.

## Hệ quả

**Dễ hơn:**

- Backfill là chuyện thường: Airflow truyền `AS_OF={{ds}}`, nên clear/rerun một ngày cũ tự tính lại đúng cửa
  sổ ngày đó. Không cần cờ riêng, không cần script riêng.
- Retry an toàn: chạy lại cùng `AS_OF` thay đúng cùng bộ partition. `retries: 2` trong DAG từ chỗ *an toàn
  tình cờ* (nhờ overwrite toàn bộ) thành *an toàn có bảo đảm*.
- Log in ra đúng danh sách partition bị thay — đọc log là tự kiểm được tính idempotent.
- `spark_batch plan` hiện rõ job nào incremental, job nào full refresh.

**Khó hơn / phải chấp nhận:**

- **Thay đổi chiều không còn hồi tố.** Trước đây full refresh nên đổi tên khách hàng là mọi dòng Silver cũ
  cập nhật theo. Giờ chỉ các dòng trong cửa sổ đổi. Với bảng fact tài chính thì "giá trị tại thời điểm giao
  dịch" thường **đúng hơn**, nhưng đây là **thay đổi ngữ nghĩa thật**, không phải tối ưu trong suốt.
  Cách chữa: chạy `--full-refresh` định kỳ (tuần/tháng).
- Dữ liệu về muộn hơn `lookback_days` bị bỏ sót tới lần full refresh.
- Đường parquet vẫn chưa nguyên tử *trong* một partition — dynamic overwrite thu hẹp bán kính xuống còn
  partition đang ghi, chứ không xoá hẳn cửa sổ rỗng. Muốn nguyên tử thật phải sang Iceberg.
- Slot CDC nay có thể bị invalidate (mục 1) — phải theo dõi và biết cách snapshot lại.

**Tài liệu cập nhật kèm:** `docs/architecture/BDP-current-state.md` (mục 4.2 #7/#13), `docs/guide/runbook.md`
(backfill + full refresh + xử lý slot invalidate).

## Phương án đã cân nhắc

| Phương án | Vì sao loại |
|---|---|
| **Chuyển hết sang Iceberg + `MERGE INTO`** | Đúng nhất về kỹ thuật, nhưng buộc Silver phụ thuộc `iceberg-rest` — thứ đang **lưu catalog trong RAM**, restart là mất namespace. Lấy nợ HA đổi nợ chi phí, không xong. |
| **Append + dedup lúc đọc** | Đẩy chi phí xuống hạ nguồn và làm mọi truy vấn Gold phức tạp hơn. Retry vẫn nhân bản. |
| **Giữ full refresh, chỉ thêm ghi nguyên tử** | Chữa được mất mát dữ liệu nhưng không chữa O(t²) — bức tường vẫn tới, chỉ chậm hơn. |
| **Cột watermark thay cho partition pruning** | Bronze *đã* time-partition sẵn (`TimeBasedPartitioner`, `path.format=year=/month=/day=/hour=`). Không dùng partition có sẵn mà đi quét cột là bỏ phí đúng thứ đắt nhất. |
| **Đưa AS_OF vào job plan JSON** | Plan ghi lúc deploy, còn cửa sổ phải tính lúc chạy. Nhét vào plan thì mọi lần chạy về sau dùng lại ngày cũ. Truyền qua env là đúng chỗ. |

## Verify live — 2026-08-14

Dữ liệu thật: Bronze 551.769 dòng. Đáng chú ý là **Bronze chỉ có 5 partition** (theo thời điểm CDC bắt
được: 07-18, 07-20, 07-23, 08-02, 08-14) trong khi **Silver có 34** (theo `posted_at`, trải từ 06-19).
Chênh lệch thang đo này chính là thứ `input_margin_days` sinh ra để xử lý — và nó cho một bài kiểm tra
mạnh: cửa sổ `[08-12..08-14]` chỉ chạm đúng 1 partition, còn 33 partition kia phải sống sót.

Đo bằng `spark/jobs/partition_census.py` (đếm dòng theo từng partition), chụp ba lần:

| Bảng | full refresh | incremental lần 1 | incremental lần 2 |
|---|---:|---:|---:|
| `silver/enriched_transactions` | 551.769 | 551.769 | 551.769 |
| `gold/daily_transaction_summary` | 537 | 537 | 537 |
| `gold/high_risk_transactions` | 171.263 | 171.263 | 171.263 |

**0/101 partition lệch** qua cả ba lần chụp. Ba bất biến đạt cùng lúc:

1. **Parity** — tổng và từng partition khớp tuyệt đối với full refresh.
2. **Không phá partition ngoài cửa sổ** — 33 partition cũ, *kể cả partition `NULL`*, giữ nguyên số dòng.
3. **Idempotent** — lần 2 ghi đúng cùng số dòng (242.255 / 66 / 75.117) và census không đổi.

Con số ghi ra cũng tự nói lên hiệu quả: incremental ghi **242.255** dòng thay vì 551.769 — đúng bằng số
dòng của partition `2026-08-14` trong baseline, không thừa không thiếu.

**Trần WAL** verify trực tiếp trên DB sống: `SHOW max_slot_wal_keep_size` = `2GB`, và
`pg_replication_slots.safe_wal_size` = 2.153.109.896 — cột này **chỉ có giá trị khi trần được đặt**, nên nó
là bằng chứng cơ chế đang hoạt động chứ không chỉ là config được đọc. Sau khi recreate container Postgres:
slot `active=t`, `wal_status=reserved`, **7/7 connector `RUNNING`** — CDC không gãy.

### Hai lỗi chỉ lộ ra khi chạy thật

Đáng ghi lại vì cả hai đều **lọt qua 58 test tĩnh**:

1. **`str | None` trong `medallion_runner`** (lỗi mới, do thay đổi này). Image Spark chạy **Python 3.8**,
   còn CI và máy dev chạy 3.12 — cú pháp union type chỉ có từ 3.10. Test xanh, job chết ngay khi submit.
   Vá bằng `from __future__ import annotations`, và thêm `tests/test_spark_jobs_python38.py` chặn cả lớp
   lỗi này (kiểm cú pháp với `feature_version=(3,8)` + bắt buộc future import ở file có annotation).
2. **`subprocess.run(text=True)` không đặt encoding** trong `spark_batch._submit` (**lỗi có sẵn từ trước**).
   Trên Windows `text=True` dùng cp1252; log Spark có byte không decode được -> thread đọc chết ->
   `proc.stdout` thành `None` -> `TypeError` khi ghép chuỗi. Đúng họ với lỗi đã vá ở `_ch_exec`/`_pg_scalar`
   ([ADR-0041](0041-harden-perimeter.md)) nhưng `spark_batch` bị bỏ sót. Nghĩa là `apply` đã mong manh từ
   trước, chỉ chưa ai chạm phải.

### Giới hạn phát hiện được khi đo

Silver có **164 dòng với `posted_at` NULL**, nằm ở partition `__HIVE_DEFAULT_PARTITION__`.
`make_date(NULL,...)` cho NULL nên chúng bị bộ lọc cửa sổ loại ra -> **incremental không bao giờ làm mới
chúng, nhưng cũng không bao giờ xoá chúng** (partition đó không nằm trong tập được ghi). Tính chất an toàn
được giữ; cái mất là chúng đóng băng ở giá trị của lần `--full-refresh` gần nhất. Hợp lý về ngữ nghĩa — một
giao dịch không có ngày nghiệp vụ thì không thuộc cửa sổ ngày nào — nhưng cần biết.

### Cách chạy lại phép kiểm

```bash
python -m dataplatform.deployers.spark_batch apply --full-refresh   # mốc chuẩn
# census -> chạy incremental -> census -> so
```

Cửa thoát luôn sẵn: `apply --full-refresh` quay về đúng hành vi cũ.
