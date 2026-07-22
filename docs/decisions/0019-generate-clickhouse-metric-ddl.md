# ADR-0019: Sinh DDL ClickHouse từ metric contract — diệt sprawl #8/#9

- **Status:** Accepted
- **Date:** 2026-07-16
- **Deciders:** Phan Trường

## Bối cảnh

Đây là sprawl gây ra **chế độ hỏng tệ nhất hệ thống**. Mỗi metric cần **3 đối tượng** ClickHouse mà
schema phải khớp **tuyệt đối**:

```
metrics.<m>         bảng đích  (ReplacingMergeTree)  <- Grafana đọc
metrics.<m>_kafka   bảng đệm   (Kafka engine)        <- kéo từ topic
metrics.<m>_mv      MV         (INSERT ... SELECT)   <- nối 2 cái trên
```

4 metric × 3 = **12 khối schema viết tay**, cộng 4 sink DDL bên Flink = 16 khối phải khớp nhau bằng
tay. Lệch một cột thì **MV bỏ dữ liệu mà không báo lỗi** — dashboard vẫn xanh, chỉ là rỗng. Mất dữ
liệu im lặng là kiểu hỏng đắt nhất.

Chặn trước đó: [ADR-0015](0015-metadata-registry-yaml-first.md) mã hóa 4 dataset OLTP, nhưng **4 metric
dataset chưa được mã hóa** — nợ từ Pha 1 của roadmap. Nó chặn cả việc này lẫn topic manifest (Pha 2).

## Quyết định

Mã hóa 4 metric thành dataset contract, rồi sinh **cả 3 đối tượng từ một `columns`** duy nhất. Hàm
`_col_block()` được cả bảng đích lẫn bảng Kafka gọi — nên chúng **không thể lệch cột**.

### Cùng một cột, hai cách render đúng

Điểm cho thấy đây không phải "copy 3 lần":

| Cột | Bảng đích | Bảng Kafka |
|---|---|---|
| `tx_type` | `LowCardinality(String)` | `String` |

Bảng đích nén được vì lưu lâu dài; bảng Kafka chỉ parse JSON rồi vứt, LowCardinality vô ích. Cùng một
sự thật logic (`low_cardinality: true`), generator biết render khác nhau tuỳ ngữ cảnh.

### `version_column` không nằm trong `columns`

`inserted_at DateTime DEFAULT now()` chỉ có ở **bảng đích**, không có ở Kafka lẫn MV. Nó là **cột phiên
bản của ReplacingMergeTree** — thuộc tầng *serving*, không phải schema *logic* của metric. Nên nó khai
trong `sinks.clickhouse.version_column`, không phải trong `columns`. Đây là ranh giới control/data
plane áp cho từng trường.

### `unsigned` / `low_cardinality` là sự thật LOGIC, không phải chi tiết engine

Có thể phản đối rằng chúng là tối ưu của ClickHouse rò vào contract. Không phải:
- `unsigned: true` = "giá trị không bao giờ âm" — sự thật về **dữ liệu** (đếm số lượng).
- `low_cardinality: true` = "rất ít giá trị phân biệt" — cũng là sự thật về dữ liệu.

ClickHouse chỉ **tình cờ khai thác được** chúng thành `UInt64` / `LowCardinality(String)`. Engine khác
sẽ khai thác cách khác. Contract vẫn engine-agnostic.

### Cửa thoát hiểm `column_types`

`rank_num` là `UInt8` (hạng 1–10). Mô hình logic chỉ biết "int không âm" — không có khái niệm "int
nhỏ". Thay vì nhồi thêm khái niệm vào mô hình logic cho **một** cột, dùng cửa thoát hiểm ép kiểu tường
minh. Mọi code generator đều cần một cửa như vậy; đo lường sức khoẻ mô hình = **đếm số lần phải dùng
nó** (hiện: 1).

## Kiểm chứng — áp DDL thật vào ClickHouse thật

Không so text (comment khác nhau là vô nghĩa). Thay vào đó **hỏi chính ClickHouse**:

1. Áp DDL **viết tay** vào ClickHouse → chụp `SHOW CREATE TABLE` cho 12 đối tượng (oracle).
2. `DROP` sạch 12 bảng.
3. Áp DDL **sinh** → chụp lại 12 đối tượng.
4. So hai bản chụp.

```
=== Neu BO QUA kafka_max_block_size (thu co y chuan hoa), con khac gi khong?
  12/12 doi tuong giong het nhau
  oracle: 12 | sinh: 12
```

Đây là oracle **mạnh hơn** diff text: ClickHouse tự nói ra schema nó thật sự tạo, nên tương đương ngữ
nghĩa được chứng minh chứ không phải suy đoán.

## Phát hiện: bản viết tay có bất nhất

Quá trình này lôi ra một lỗi **chưa ai biết**:

| Bảng | `kafka_max_block_size` |
|---|---|
| `timeseries_kafka` | `1048576` |
| `kpi_kafka` | **thiếu** |
| `breakdown_kafka` | **thiếu** |
| `topn_kafka` | **thiếu** |

Bốn thứ đáng lẽ giống nhau, một cái lệch — điển hình của copy-paste rồi quên propagate. **Đã chuẩn hoá:
cả 4 cùng khai tường minh.** Đây chính là giá trị metadata-driven: nó **ép đồng đều**, và lộ ra bất
nhất mà mắt người bỏ qua.

Rủi ro của việc chuẩn hoá: thấp. Mặc định của ClickHouse (`max_insert_block_size` = 1048449) gần như
bằng 1048576, và 12 bảng metric **chưa tồn tại** lúc cắt chuyển nên không đổi hành vi gì đang chạy.
Khai tường minh > phụ thuộc mặc định server (mặc định có thể đổi khi nâng version).

## Hệ quả

**Dễ hơn:**
- Thêm/sửa cột metric = sửa **một** `columns` → cả 3 đối tượng tự khớp. Hết cảnh MV bỏ dữ liệu âm thầm.
- Thêm một metric = thêm 1 file YAML → sinh 3 đối tượng, không viết SQL.
- Trả xong nợ Pha 1 (4 metric dataset) → **mở khoá topic manifest** (Pha 2).
- Control plane phủ **11 artifact**.

**Khó hơn / phải chấp nhận:**
- `01_schema.sql` + `02_kafka_consumers.sql` giờ là **file sinh** — sửa phải sửa contract (có header
  cảnh báo).
- Còn **một nửa** của sprawl #8: sink DDL bên Flink (`lane1_dashboard.py`) **vẫn viết tay**, nên vẫn có
  thể lệch với ClickHouse. Chỉ hết hẳn khi Flink runner sinh cả hai đầu từ cùng spec (Pha 3).
- Mô hình type mapping (logic → ClickHouse) là tri thức engine sống trong control plane; thêm engine
  mới = thêm một bảng ánh xạ.

## Phương án đã cân nhắc

- **Tái tạo y hệt bất nhất `kafka_max_block_size`** (thêm override cho riêng timeseries). Bị loại: đó là
  đóng băng một lỗi vào spec. Generator sinh ra để **ép đồng đều**; giữ lại bất nhất là phản mục đích.
- **So DDL bằng text.** Bị loại: comment/format khác nhau là nhiễu. So schema ClickHouse **thật sự tạo
  ra** mới là oracle đúng.
- **Đưa `inserted_at` vào `columns`.** Bị loại: nó sẽ lọt vào bảng Kafka và MV — sai. Nó là chi tiết
  serving, không phải schema metric.
- **Bỏ `unsigned`/`low_cardinality`, dùng `column_types` cho mọi cột.** Bị loại: biến contract thành
  DDL ClickHouse trá hình, mất tính engine-agnostic. Cửa thoát hiểm phải là ngoại lệ, không phải lối chính.
- **Làm Pha 3 (Flink runner) trước.** Bị hoãn: rủi ro "Cao" theo roadmap. Pha 4 rủi ro "Trung bình" và
  **có oracle** (12 bảng đang chạy) — làm cái chắc chắn trước.
