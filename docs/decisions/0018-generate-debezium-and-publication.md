# ADR-0018: Sinh Debezium connector + publication SQL từ một nguồn — diệt sprawl #2/#3

- **Status:** Accepted
- **Date:** 2026-07-16
- **Deciders:** Phan Trường

## Bối cảnh

[ADR-0015](0015-metadata-registry-yaml-first.md) chứng minh mô hình metadata-driven chạy được trên ES
sink. Bước này giải quyết **sprawl nguy hiểm nhất** trong bảng audit
([`../architecture/BDP-current-state.md`](../architecture/BDP-current-state.md) §3): danh sách bảng CDC
bị khai **tay ở hai nơi độc lập**:

- `postgres/init/04_publication.sql` → `CREATE PUBLICATION ... FOR TABLE ...` (sprawl #2)
- `debezium/postgres-connector.json` → `table.include.list` (sprawl #3)

Nếu hai danh sách lệch nhau — thêm bảng vào connector mà quên thêm vào publication — thì bảng đó
**im lặng không có CDC**, không lỗi, không cảnh báo. Đây là chế độ hỏng tệ nhất: mất dữ liệu mà mọi
thứ trông vẫn xanh.

## Quyết định

Sinh **cả hai** artifact từ **cùng một nguồn** — danh sách dataset có `source.type = cdc_debezium`
trong registry. Vì cùng đọc một nguồn, chúng **không thể lệch nhau** được nữa.

- `dataplatform/generators/debezium.py` → `postgres-connector.json`, với `table.include.list` gộp từ
  mọi dataset CDC (generator **fan-in**, giống `s3_sink`).
- `dataplatform/generators/postgres_publication.py` → `04_publication.sql`, dùng lại đúng hàm
  `cdc_datasets()` của debezium.py.

Kiểm chứng khớp bằng chính code:
```
connector table.include.list: [accounts, customers, transactions, transfers]
publication FOR TABLE       : [accounts, customers, transactions, transfers]
KHỚP NHAU: True   ← vì cùng suy từ registry
```

## Điểm kỹ thuật đáng ghi

### Artifact dạng TEXT, không phải JSON
8 artifact trước đều là JSON — `check` so **ngữ nghĩa** (parse dict). Publication là **SQL**, không so
dict được. Đã mở rộng `check` để rẽ theo loại:

| Loại | Cách so | Vì sao |
|---|---|---|
| dict (JSON) | so ngữ nghĩa sau khi parse | file có dòng trống + thứ tự khoá tuỳ người; ép byte-match là giòn |
| str (SQL/text) | so **nguyên văn** | file này **control plane sở hữu hoàn toàn**, không công cụ ngoài format lại → byte-match hợp lý và chặt hơn |

### `table.include.list` là khoá "so như set"
Thêm vào cùng nhóm với `topics`: nó là danh sách ngăn phẩy mà thứ tự không mang nghĩa. Không thế thì
generator sắp thứ tự khác người viết sẽ báo lệch giả.

### Đây là cắt chuyển THẬT, không phải diff-rỗng
Với ES/S3 sink, bản sinh **byte-equivalent** bản viết tay → cắt chuyển an toàn tuyệt đối. Publication
thì **khác**: bản sinh thêm header "sinh tự động", bỏ mấy câu `SELECT` verify (đã có trong
[`../guide/cdc-and-connectors.md`](../guide/cdc-and-connectors.md) §5), chuẩn hoá GRANT. Nên `check`
báo `[KHÁC]` — đúng vai trò "plan": cho xem sẽ đổi gì. Sau khi **kiểm chứng ngữ nghĩa** (đúng 4 bảng,
đúng publish mode, đúng GRANT, và khớp connector), mới `write` để cắt chuyển.

## Hệ quả

**Dễ hơn:**
- Thêm/bớt bảng CDC = sửa `source` trong **một** contract → cả connector lẫn publication tự đúng theo.
- Sprawl #2/#3 **chết có bằng chứng**: hai danh sách không thể lệch vì cùng một nguồn.
- Control plane giờ phủ **9 artifact** trên 4 loại (ES sink, S3 sink, DLQ, Debezium, publication).

**Khó hơn / phải chấp nhận:**
- Publication SQL **không** còn chứa câu verify — người vận hành xem verify trong guide, không trong
  init script (init script không phải chỗ cho SELECT chẩn đoán).
- Có thêm loại artifact (text) → `check`/`write` phức tạp hơn một chút.
- `04_publication.sql` giờ là **file sinh** — sửa nó phải sửa contract, không sửa tay (có header cảnh
  báo, giống `dlq_topics.json`).

## Phương án đã cân nhắc

- **Chỉ sinh connector, để publication viết tay.** Bị loại: đó là làm nửa vời — hai danh sách **vẫn**
  lệch được, tức **không** diệt được sprawl. Phải sinh cả hai mới có ý nghĩa.
- **Sinh publication nhưng giữ nguyên byte-for-byte file cũ** (kể cả câu verify). Bị loại: init script
  chứa `SELECT` chẩn đoán là rác — output chạy lúc init DB không đi đâu cả. Bỏ đi sạch hơn.
- **So SQL bằng cách parse ra AST.** Bị loại: quá nặng cho một publication đơn giản. Vì control plane
  sở hữu canonical form, so nguyên văn là đủ chặt.
- **Để Debezium tự tạo publication** (`publication.autocreate.mode=filtered`). Bị loại: khi đó Debezium
  tự sửa publication → **hai** thứ cùng quản một publication → drift. Giữ `disabled`, ta quản bằng SQL
  sinh.
