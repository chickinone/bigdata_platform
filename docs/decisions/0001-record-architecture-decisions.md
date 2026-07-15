# ADR-0001: Ghi lại quyết định kiến trúc dưới dạng ADR

- **Status:** Accepted
- **Date:** 2026-07-15
- **Deciders:** Phan Trường (cùng Claude Code)

## Bối cảnh

Dự án này được làm bởi nhiều người và nhiều phiên agent không chia sẻ trí nhớ với nhau hay theo thời
gian. Các tài liệu trong `docs/architecture/` mô tả trạng thái **hiện tại** khá tốt, nhưng không nói
*vì sao* các quyết định trong quá khứ lại được chọn như vậy, hay phương án nào đã bị loại.

Với hệ thống này, khoảng trống đó đặc biệt đắt. Nhiều thứ trông như lỗi nhưng thật ra là lựa chọn có
chủ ý:

- `decimal.handling.mode=string` khiến mọi cột tiền tệ thành chuỗi — trông như bug, nhưng là cố ý.
- `accounts` và `transfers` dùng `REPLICA IDENTITY FULL` còn hai bảng kia thì không — trông như thiếu
  nhất quán, nhưng có lý do.
- Bốn file `lane1_*.py` trông như job chạy được, nhưng là di sản và **không được chạy**.
- Flink ghi ra Kafka rồi ClickHouse mới kéo về — trông như thừa một chặng, nhưng là chủ đích.

Không có dấu vết lý do, người đến sau (người hay agent) sẽ hoặc lật lại quyết định đã chốt, hoặc vi
phạm nó mà không biết đó là chủ ý.

## Quyết định

Ghi lại mọi quyết định làm thay đổi ranh giới trách nhiệm giữa các công cụ, thay đổi mô hình dữ liệu,
hoặc đảo ngược một quyết định trước — dưới dạng ADR đánh số trong `docs/decisions/`, theo
[`template.md`](template.md). **Không bao giờ sửa quyết định của ADR cũ tại chỗ** — viết ADR mới để
supersede nó và cập nhật status của cái cũ.

ADR-0002 đến ADR-0013 là **hồi tố** — chúng ghi lại các quyết định đã tồn tại trong code từ trước,
dựng lại từ chính code và comment. Đánh dấu vậy để người đọc biết chúng được viết sau khi việc đã rồi.

## Hệ quả

- Thêm chút chi phí quy trình cho các thay đổi ở tầng kiến trúc.
- Cho mọi phiên làm việc sau (người hoặc agent) cách khôi phục *vì sao* thiết kế lại như vậy, không
  chỉ *nó đang là gì* — đây là vấn đề chính cần giải.
- `docs/README.md` liệt kê toàn bộ ADR và phải cập nhật khi thêm cái mới.

## Phương án đã cân nhắc

- **Chỉ dựa vào lịch sử Git / CHANGELOG.** Bị loại: commit message là theo **thay đổi** và theo trình
  tự thời gian, không phải theo **quyết định**. Chúng không cho một bản ghi bền vững, tra cứu được cho
  câu hỏi "vì sao nó lại thế này" khi đã có hàng chục commit không liên quan chen vào giữa.
- **Viết lý do ngay trong `docs/architecture/*.md`.** Bị loại: các tài liệu đó mô tả kiến trúc hiện
  tại và cần giữ gọn; nhét lý do lịch sử cùng phương án bị loại vào sẽ làm rối phần mà người mới cần
  đọc trước nhất.
- **Viết comment trong code.** Bị loại: comment giải thích *dòng lệnh này*, không giải thích *lựa chọn
  xuyên nhiều file*. Quyết định "Flink ghi qua Kafka thay vì ghi thẳng ClickHouse" không thuộc về file
  nào cả — nó thuộc về khoảng trống *giữa* các file.
