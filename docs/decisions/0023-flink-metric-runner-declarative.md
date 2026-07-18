# ADR-0023: Flink runner tổng quát khai báo — diệt sprawl #6/#8

- **Status:** Accepted (phần sinh + oracle SQL); cutover runtime đang chờ
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
- **kiểu source theo MÃ HOÁ TRÊN DÂY**, không phải kiểu logic: `amount` là `decimal` nhưng
  `encoded_as: string` → Flink `STRING` (ADR-0003). Đây là chỗ verifier Avro (ADR-0022) và runner cùng
  đọc một sự thật.
- **sink DDL** sinh từ **đúng cột contract metric** — cùng nguồn cột với DDL ClickHouse (ADR-0019), nên
  **không thể lệch**. Đây là chỗ đóng nửa Flink của sprawl #8.

### Kiểm chéo: cột spec PHẢI khớp cột sink

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

## Việc còn lại (cutover runtime — chưa xong)

Đúng kỷ luật strangler-fig, **chưa xoá `lane1_dashboard.py`**:

1. Submit runner vào Flink (`flink run -py metric_runner.py`) với **group.id mới** (`flink-metrics-runner`)
   → song song job cũ, không giành offset. (Đang chờ Docker Desktop lên lại — nó tắt giữa lúc submit.)
2. Xác nhận job RUNNING = SQL sinh **hợp lệ** (parse + plan trong Flink — lớp kiểm mắt không thấy).
3. Có traffic thật (hiện `transactions` rỗng): so đầu ra runner mới với job cũ **trên cùng dữ liệu**;
   `ReplacingMergeTree` nuốt trùng nên chồng lấn vô hại.
4. Khớp → cắt chuyển → **xoá `lane1_dashboard.py`**.

## Hệ quả

**Dễ hơn:** thêm metric = 1 file YAML spec, không Python. Source ROW + sink DDL + INSERT đều sinh từ
metadata; sink không thể lệch ClickHouse.

**Khó hơn / phải chấp nhận:**
- **Lane 3 (fraud) chưa đụng**: logic detector có state (velocity, failed-storm) không tổng quát hoá
  được — sẽ giữ là code + tham số hoá riêng, và bỏ `ds.print` spam log (gap #6).
- Generator giờ mang tri thức dựng SQL (window/rank) — thêm dạng pipeline mới = thêm code sinh.
- Job plan là artifact **runtime** (có group.id/bootstrap cụ thể) → không commit, sinh lúc deploy.

## Phương án đã cân nhắc

- **Lai (cấu trúc + cửa thoát SQL)** và **để SQL trong spec**: loại theo lựa chọn — đi khai báo hoàn toàn
  cho thuần metadata, dù topn phải thêm primitive `rank` và rủi ro sinh SQL cao hơn (đã bù bằng oracle SQL).
- **Source ROW gồm mọi cột contract**: loại — sinh từ cột tham chiếu cho ROW tối thiểu và tự dọn cột chết.
- **Xoá `lane1` ngay khi SQL khớp**: loại — chưa có parity runtime dưới traffic. Giữ lưới cũ tới khi chứng minh.
