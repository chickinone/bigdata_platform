# ADR-0046: Verifier OpenMetadata — kéo mảnh cuối vào vòng lặp

- **Status:** Accepted (tầng 1 verify live; tầng 2 chưa — xem cuối)
- **Date:** 2026-09-01
- **Deciders:** Phan Truong

## Bối cảnh

Sau [ADR-0045](0045-orphan-gc-state.md), vòng lặp đã khép cho mọi engine: `metadata/` → `cli apply`
(có thứ tự + gate) → `cli verify` (7 verifier, chạy theo lịch) → GC.

Còn đúng **một mảnh nằm ngoài**: OpenMetadata.

```
openmetadata_ingestion   Exited (137) 8 ngày trước   <- OOM-kill
openmetadata_postgresql  Exited (0)   8 ngày trước
```

Nó không nằm trong chuỗi `cli apply` mặc định (chỉ bật bằng `--with-openmetadata`, vì OM là **phiên
riêng** cần dừng stack chính để nhường RAM), và cũng không có verifier nào. Hệ quả: **OM chết 8 ngày mà
không ai biết**, và catalog mà người ta mở ra xem phản ánh trạng thái hơn một tuần trước.

Một catalog cũ mà trông như mới **tệ hơn không có catalog** — nó khiến người đọc tin vào thứ sai. Đây
đúng loại lỗi cả loạt ADR gần đây đang chữa: tín hiệu nói dối.

## Quyết định

Thêm `verifiers/om_catalog.py` và đưa vào `VERIFIERS`. **Không** nhét OM vào `cli apply` mặc định — ràng
buộc RAM là thật, và một bước luôn hỏng vì service cố ý tắt sẽ làm cả chuỗi vô dụng.

Verifier chia **hai tầng theo thứ nó cần**:

| Tầng | Kiểm gì | Cần OM sống? |
|---|---|---|
| 1 | **độ cũ** — vân tay `graph.json` đã đẩy vs hiện tại | **không** |
| 2 | **nội dung** — bảng trên OM vs bảng `graph.json` khai | có |

### Tầng 1 là phần đắt giá

`openmetadata apply` nay ghi vào `.platform-state.json`:

```json
"openmetadata": { "graph_fingerprint": "f6534aaa203ae723", "applied_at": "..." }
```

Verifier so vân tay đó với `sha256(graph.json)` hiện tại. Khác nhau = catalog đang cũ.

Điều này trả lời được câu hỏi quan trọng nhất — *"catalog có đang cũ không"* — **mà không cần OM chạy**.
Mấu chốt nằm ở đó: nếu chỉ kiểm được lúc OM sống, thì với một service thường xuyên tắt, ta gần như
không bao giờ kiểm.

### Mã thoát: OM tắt KHÔNG được làm cổng đỏ

| Tình huống | Mã | Vì sao |
|---|---|---|
| chưa từng `apply` | 0 | catalog chưa tồn tại thì không có gì để lệch |
| catalog cũ | 0 + **chú ý** | lag là trạng thái *bình thường* của một view làm mới theo yêu cầu |
| OM tắt | 0 | phiên riêng — tắt là mặc định, không phải sự cố |
| OM sống, **thiếu** bảng | **1** | `apply` hỏng nửa chừng, hoặc ai đó xoá trên UI |
| OM sống, **thừa** bảng | 0 + chú ý | instance OM có thể chứa entity của project khác |

Nếu "OM tắt" trả 1 thì `cli verify` đỏ mỗi giờ, và **một cổng luôn đỏ là một cổng bị bỏ qua** — cùng lý
do đã chọn `--no-git` cho gitleaks ([ADR-0041](0041-harden-perimeter.md)) và đã tách exit 1 khỏi exit 3
trong `cli verify` ([ADR-0043](0043-cold-rebuild-findings.md)). `test_om_catalog_verifier.py` khoá ràng
buộc này lại.

### Không chép lại danh sách bảng

Verifier dùng thẳng `om._fqn`, `om._schema_of`, `om._table_name`, `om.GRAPH_PATH` của deployer. Nếu tự
viết lại logic đặt tên, hai bên sẽ lệch nhau về "bảng nào phải có" — đúng thứ sprawl cả dự án đang diệt.
Có test chặn việc chép lại.

Verifier **chỉ đọc**: có test cấm mọi `PUT`/`POST`/`PATCH`/`DELETE`. Hai nơi cùng ghi lên OM thì không
còn biết trạng thái nào đúng.

## Kèm theo: `dataplatform/state.py`

`.platform-state.json` giờ có hai người dùng (connector và OM), nên logic đọc/ghi tách thành module dùng
chung thay vì để mỗi deployer một bản sao. `save()` đọc-sửa-ghi cả file, không ghi đè — nếu ghi đè thì
deployer sau sẽ xoá mất state của deployer trước.

## Kiểm chứng

Tầng 1, chạy thật:

```
chưa từng đẩy  -> "bỏ qua — chưa chạy openmetadata apply lần nào"           exit 0
giả lập đã đẩy 24/08 với vân tay khác:
  -> [chú ý] catalog ĐANG CŨ: graph.json đã đổi kể từ lần đẩy cuối          exit 0
  -> OM không truy cập được (URLError) — BÌNH THƯỜNG, phiên riêng
cli verify -> 8/8 verifier DAT
```

**Tầng 2 chưa verify live.** Đường "OM sống, đối chiếu nội dung" chưa chạy trên OM thật, vì bật OM đòi
dừng stack chính để nhường RAM — một thao tác nặng ngoài phạm vi thay đổi này. Đường lỗi và đường
"không nối được" đã chạy đúng. Phải chạy `python -m dataplatform.verifiers.om_catalog` trong một phiên
OM trước khi tin tầng 2.

## Hệ quả

- Dễ hơn: `cli verify` (chạy mỗi giờ) nay nói được "catalog đang cũ" mà không cần bật OM.
- Dễ hơn: **không còn mảnh nào của hệ thống nằm ngoài vòng lặp.**
- Khó hơn: mất `.platform-state.json` thì tầng 1 quay về "chưa từng đẩy" — mất khả năng phát hiện cũ cho
  tới lần `apply` sau. Không nguy hiểm, chỉ là mù tạm thời.
- Có chủ đích **không** làm: OM vẫn ngoài `cli apply` mặc định. Ràng buộc RAM là thật.

## Phương án đã cân nhắc

- **Nhét OM vào `cli apply` mặc định** — loại: OM cần stack chính tắt; bước này sẽ hỏng gần như mọi lần
  và làm cả chuỗi mất giá trị.
- **Chỉ kiểm khi OM sống** — loại: OM thường tắt, nên verifier sẽ gần như không bao giờ chạy thật. Đúng
  cái bẫy "bộ dò tồn tại nhưng không ai gọi" đã gặp ở ADR-0043.
- **Coi catalog cũ là lệch (exit 1)** — loại: `graph.json` đổi thường xuyên hơn nhiều so với nhịp chạy
  OM, nên cổng sẽ đỏ gần như vĩnh viễn rồi bị bỏ qua.
- **So bằng thời điểm sửa file thay vì vân tay nội dung** — loại: `git checkout` làm mtime nhảy mà nội
  dung không đổi, sinh báo động giả.
