# ADR-0023: Flink runner tổng quát khai báo — diệt sprawl #6/#8

- **Status:** Accepted — runner metric đã cắt chuyển, `lane1_dashboard.py` đã xoá
- **Date:** 2026-07-18
- **Deciders:** Phan Trường

## Bối cảnh

`flink/jobs/lane1_dashboard.py` viết tay: 1 source `ROW<...>` (sprawl #6, lặp ở nhiều file Flink) + 4
sink DDL (nửa Flink của sprawl #8 — **có thể lệch schema ClickHouse → mất dữ liệu âm thầm**) + 4 câu
INSERT. Thêm một metric = viết Python mới. Đây là khối lớn nhất còn lại của lộ trình, rủi ro "Cao".

## Quyết định

Chọn **cấu trúc khai báo hoàn toàn** (thay vì lai hay để SQL trong spec): mỗi metric là một pipeline
spec khai `window / filter / dimensions / aggregations / rank`, và generator dựng **toàn bộ** Flink SQL.
Một runner mỏng (`flink/jobs/metric_runner.py`) chỉ đọc job plan sinh sẵn và submit — không chứa logic.

Tách bạch **sinh (host) / thực thi (container)**: SQL sinh trên host nơi có `dataplatform` + deps; container
Flink không cần jsonschema/pyyaml. Thêm/sửa metric = sửa YAML, không đụng Python.

### Ba mẩu sinh, mỗi mẩu bịt một chỗ

- **source ROW** sinh từ cột **thật sự được tham chiếu** (quét `` `after`.X `` trong filter/dimensions/
  aggregations). Diệt sprawl #6, và tự loại **cột chết**: ROW viết tay cũ có `transaction_id`, `currency`
  mà không INSERT nào dùng — bản sinh bỏ chúng.
- **kiểu source theo mã hoá trên dây**, không phải kiểu logic: `amount` là `decimal` nhưng
  `encoded_as: string` → Flink `STRING` (ADR-0003). Đây là chỗ verifier Avro (ADR-0022) và runner cùng
  đọc một sự thật.
- **sink DDL** sinh từ **đúng cột contract metric** — cùng nguồn cột với DDL ClickHouse (ADR-0019), nên
  **không thể lệch**. Đây là chỗ đóng nửa Flink của sprawl #8.

### Kiểm chéo: cột spec phải khớp cột sink

`[window_start, window_end] + rank + dimensions + aggregations` phải **bằng đúng** cột của contract sink
(tên + thứ tự). Lệch → generator dừng. Spec và sink không thể trôi xa nhau âm thầm.

### `rank` cho topn

Ca duy nhất cần xếp hạng: spec khai `rank: {partition_by, order_by, keep, as}` → sinh
`ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) ... WHERE rank <= keep`, bọc ngoài phần aggregate.

## Kiểm chứng — oracle SQL (tĩnh, không cần chạy)

Rủi ro của hướng khai báo là SQL tự sinh lệch tinh vi so với job cũ. Chặn bằng cách **so SQL sinh với SQL
viết tay** trong `lane1_dashboard.py` (job đã chạy production = oracle):

- **4 sink DDL: khớp tuyệt đối** (kể cả `rank_num BIGINT` — nhận `ROW_NUMBER`).
- **4 INSERT: tương đương ngữ nghĩa** (window/filter/group_by/aggregations trùng khớp; topn đặt tên
  `rank_num` trực tiếp thay `rn AS rank_num` — tương đương).
- **source ROW: 4 cột thay vì 6** — chỉ khác ở 2 cột chết được loại. Output-equivalent.

Vì SQL tương đương, **đầu ra khớp theo cấu tạo** → nền cho bước chạy-song-song. Kiểm chéo cột được test âm
(đổi alias aggregation → generator chặn đúng).

## Cutover runtime — đã hoàn thành (parity với ground truth)

Chạy đầy đủ quy trình strangler-fig cho streaming:

1. [x] Submit runner (`flink run -py metric_runner.py`, group `flink-metrics-runner` → song song, không
   giành offset). Job **RUNNING**, mọi vertex RUNNING — SQL sinh **hợp lệ** (parse + plan). Đặc biệt
   `WindowRank` vertex xác nhận primitive `rank` của topn sinh đúng operator.
2. [x] **Parity với ground truth** (mạnh hơn so với job cũ): seed 15 deposit + 5 withdrawal (biết trước
   kết quả), đợi watermark đẩy cửa sổ TUMBLE fire. Runner phát:
   ```
   metrics.timeseries [09:31–09:32]: deposit 15/1500, withdrawal 5/250
   ClickHouse metrics.timeseries:     deposit 15/1500, withdrawal 5/250
   ```
   Khớp seed tuyệt đối — runner đọc Avro thật (`amount` STRING → CAST), window, aggregate, ghi Kafka,
   chảy vào ClickHouse, đều đúng.
3. [x] Cắt chuyển → **`lane1_dashboard.py` đã xoá**. Runner là job metric duy nhất.
4. [x] Deployer `deployers/flink_metrics.py` (`plan`/`apply`) làm việc submit lặp lại được — thay
   `flink run` thủ công.

## Hệ quả

**Dễ hơn:** thêm metric = 1 file YAML spec, không Python. Source ROW + sink DDL + INSERT đều sinh từ
metadata; sink không thể lệch ClickHouse.

**Lane 3 (fraud) — đã làm cùng cách:** detector có state (velocity, failed-storm) giữ **verbatim là code**
(không tổng quát hoá được), nhưng `fraud_runner.py` sinh **source DDL** từ `fraud.yaml` (ROW khớp
byte-for-byte lane3 cũ) và **tham số hoá** ngưỡng/cửa sổ/topic; bỏ 2 `ds.print` spam (gap #6). Runtime:
seed 8 giao dịch/account → `VELOCITY_FRAUD tx_count=8` khớp ground truth. Đã xoá `lane3_fraud_detection.py`.
Sprawl #6 **hết hẳn**.

**Ghi chú vận hành (phát hiện lúc làm):** Flink session job **không sống sót qua restart jobmanager**
(single-node, không HA). Giữa lúc làm, stack bị cycle → cả hai job biến mất. Nhưng vì mọi thứ sinh từ
metadata, **recovery là một lệnh**: `connectors apply` (khôi phục CDC) + `flink_metrics apply` (resubmit 2
runner). Auto-resubmit lúc khởi động thuộc orchestration (Pha 7).

**Khó hơn / phải chấp nhận:**
- Generator giờ mang tri thức dựng SQL (window/rank) — thêm dạng pipeline mới = thêm code sinh.
- Job plan/config là artifact **runtime** (có group.id/bootstrap cụ thể) → không commit, sinh lúc deploy.
- Deployer `apply` submit mới, chưa huỷ job cũ (cần `flink cancel` trước nếu đang chạy) — reconcile: Pha 7.

## Phương án đã cân nhắc

- **Lai (cấu trúc + cửa thoát SQL)** và **để SQL trong spec**: loại theo lựa chọn — đi khai báo hoàn toàn
  cho thuần metadata, dù topn phải thêm primitive `rank` và rủi ro sinh SQL cao hơn (đã bù bằng oracle SQL).
- **Source ROW gồm mọi cột contract**: loại — sinh từ cột tham chiếu cho ROW tối thiểu và tự dọn cột chết.
- **Xoá `lane1` ngay khi SQL khớp**: loại — chưa có parity runtime dưới traffic. Giữ lưới cũ tới khi chứng minh.
