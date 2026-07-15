# ADR-0014: Chọn metadata-driven làm đích kiến trúc

- **Status:** Accepted
- **Date:** 2026-07-15
- **Deciders:** Phan Trường (cùng Claude Code)

## Bối cảnh

Nền tảng chạy được end-to-end và bao phủ rất nhiều pattern kỹ thuật thật. Nhưng audit ở
[`../architecture/BDP-current-state.md`](../architecture/BDP-current-state.md) §3 đo được một con số cụ
thể: **một** thực thể (`transactions`) có schema bị khai báo lại bằng tay ở **13 nơi**, trải trên 5
công cụ.

Hệ quả đo được, không phải cảm tính:

- Thêm **một cột** = sửa Postgres DDL → tối đa 6 file Flink → 3 khối ClickHouse → `SELECT` Spark →
  cân nhắc mapping ES. Rất dễ bỏ sót.
- Thêm **một bảng** = đụng tối thiểu 8 file ở 5 công cụ.
- Lệch cột MV ClickHouse = **bỏ dữ liệu không báo lỗi**. Dashboard vẫn xanh, chỉ là rỗng.
- Không trả lời tự động được: "cột `amount` chảy tới đâu?", "PII nằm ở đâu?", "ai sở hữu dataset này?"

Đây không phải vấn đề thiếu công cụ — các engine đều đúng và đủ. Đây là vấn đề **cách khai báo và vận
hành cấu hình**.

## Quyết định

Chọn **metadata-driven** làm đích kiến trúc: khai báo mỗi thực thể/pipeline **một lần duy nhất** dưới
dạng data contract, rồi **sinh** mọi artifact vận hành từ đó.

Áp dụng theo lộ trình 9 pha ở
[`../roadmap/BDP-metadata-driven-roadmap.md`](../roadmap/BDP-metadata-driven-roadmap.md), dùng chiến
lược **strangler-fig**: với mỗi thành phần — mã hoá hiện trạng thành metadata → sinh artifact →
**diff** artifact sinh vs viết tay → cắt chuyển → xoá file viết tay. Không dừng hệ thống.

Hai ràng buộc bắt buộc:

1. **Pha 0 là điều kiện chặn.** Vá các khoảng trống chức năng (§4.1) **trước khi** tự động hoá bất cứ
   thứ gì. Tự động hoá trên nền còn hỏng chỉ làm chỗ hỏng lan nhanh hơn.
2. **Bắt đầu bằng YAML + Git**, không phải registry service. Chỉ lên catalog/DB khi thật cần.

## Hệ quả

**Dễ hơn:**
- Thêm cột/bảng/metric = sửa **một** file contract.
- Schema Flink output == schema ClickHouse input được đảm bảo bằng **generator**, không bằng con người
  — triệt tận gốc chế độ hỏng tệ nhất của hệ thống.
- Thay đổi review được bằng diff có nghĩa; gate được bằng CI.
- Lineage và ownership suy ra được tự động từ pipeline spec.

**Khó hơn / phải chấp nhận:**
- **14–20 tuần cho 1–2 kỹ sư.** Đây là đầu tư lớn, không phải việc cuối tuần.
- Thêm một lớp gián tiếp: đọc contract + generator thay vì đọc thẳng artifact. Người mới phải học
  control plane trước khi sửa được cái gì.
- **Pha 3 rủi ro cao** — tổng quát hoá Flink là phần khó nhất. Fraud (DataStream API) có thể **không**
  tổng quát hoá được; kế hoạch là tham số hoá thay vì generalize hoàn toàn.
- Generator sinh sai có thể hỏng hàng loạt → bước **diff** trước khi apply là bắt buộc, không phải
  tuỳ chọn.
- Rủi ro over-engineering nếu nhảy thẳng lên registry/catalog quá sớm.

**Giá trị đến sớm:** sau **Pha 2** (~4 tuần) đã hết trùng lặp ở tầng ingestion — đó là điểm quyết định
có đi tiếp hay không.

## Phương án đã cân nhắc

- **Giữ nguyên hiện trạng.** Bị loại: hệ thống hiện chỉ có 4 thực thể và 4 metric. Trùng lặp đã đau ở
  quy mô đó; thêm một domain nữa là không quản nổi. Đây là quyết định đúng **nếu** dự án dừng ở mức
  demo — nhưng đích đã đặt là production.
- **Chỉ dọn dẹp, không metadata-driven** (gỡ file trùng, gom `ROW<...>` vào module Python chung). Bị
  loại một phần: rẻ hơn nhiều và giải quyết được sprawl #6 (Flink). Nhưng **không** giải được sprawl
  giữa các *công cụ* — hằng số Python không sinh ra được DDL ClickHouse hay JSON connector. Vẫn nên làm
  ngay ở Pha 0 như một bước trung gian.
- **Mua công cụ có sẵn** (dbt cho transform, Airbyte cho ingestion). Bị loại: không cái nào phủ được cả
  Flink SQL + ClickHouse MV + Debezium + Spark + Iceberg trong một mô hình. Sẽ đổi 13 nơi khai báo lấy
  13 nơi khai báo **khác**, cộng thêm ràng buộc vào nhà cung cấp.
- **Nhảy thẳng lên OpenMetadata/DataHub.** Bị loại: catalog là **index để tra cứu**, không phải nguồn
  sự thật. Cài catalog mà không có contract sẽ chỉ ra một danh mục đẹp của cùng đống sprawl đó.
