# ADR-0016: Đặt tên thư mục control plane là `dataplatform/`, không phải `platform/`

- **Status:** Accepted *(sửa đề xuất trong roadmap)*
- **Date:** 2026-07-15
- **Deciders:** Phan Trường

## Bối cảnh

[Roadmap](../roadmap/BDP-metadata-driven-roadmap.md) §2.3 đề xuất cấu trúc thư mục:

```text
platform/
  generators/
  deployers/
  schemas/
  runtime/
```

`platform` là **module chuẩn của Python** (`C:\Python314\Lib\platform.py`) — nơi cung cấp
`platform.python_version()`, `platform.system()`... Rất nhiều thư viện gọi `import platform` để dò
phiên bản OS/Python.

Đặt một thư mục cùng tên ở gốc repo là đặt mìn.

## Ranh giới chính xác của vấn đề

Đã kiểm chứng trực tiếp, vì lời đồn "trùng tên stdlib là hỏng" chỉ đúng một nửa:

**Thư mục `platform/` KHÔNG có `__init__.py`** → an toàn:
```text
import platform  ->  C:\Python314\Lib\platform.py
```
Lý do: thư mục không có `__init__.py` chỉ là *namespace portion*. Python ghi nhận nó rồi **vẫn quét
tiếp** các path entry còn lại; tìm thấy module thật (`platform.py` trong stdlib) thì module thật
thắng. Namespace package là phương án chót.

**Thư mục `platform/` CÓ `__init__.py`** → hỏng ngay:
```text
import platform  ->  D:\bigdata-platform\platform\__init__.py
AttributeError: module 'platform' has no attribute 'python_version'
  (consider renaming 'D:\bigdata-platform\platform\__init__.py' since it has the
   same name as the standard library module named 'platform' and prevents
   importing that standard library module)
```
Nó thành *regular package*, được tìm thấy ở path entry đầu tiên và thắng ngay lập tức. Chính Python
cũng gợi ý đổi tên.

Đây không phải rủi ro lý thuyết: control plane **chắc chắn sẽ** cần là package thật để viết
`from dataplatform.registry import load_datasets`. Tức là ta chắc chắn sẽ thêm `__init__.py`, tức là
chắc chắn giẫm mìn. Và triệu chứng sẽ rất khó lần: một thư viện thứ ba nào đó gọi `import platform`
rồi chết với `AttributeError` không liên quan gì tới code của mình.

## Quyết định

Đặt tên thư mục control plane là **`dataplatform/`**.

```text
dataplatform/
  __init__.py
  registry.py          # đọc + validate contract
  cli.py               # check / write / show
  schemas/             # JSON Schema cho chính metadata
  generators/
    es_sink.py
```

Sửa luôn đề xuất trong roadmap §2.3 để người sau không lặp lại.

## Hệ quả

- Không còn xung đột với stdlib; `import platform` và `import dataplatform` sống chung.
- Lệch khỏi tên trong roadmap gốc — nên ADR này tồn tại để giải thích, tránh ai đó "sửa lại cho khớp
  tài liệu".
- Tên `dataplatform` cũng mô tả đúng hơn: đây là control plane của *data* platform, không phải thư mục
  chứa thứ liên quan tới nền tảng chạy.

## Bài học tổng quát

Trước khi đặt tên package Python trùng một từ phổ thông, kiểm tra nó có phải stdlib không:

```bash
python -c "import <ten>; print(<ten>.__file__)"
```

Các tên dễ vấp cùng loại: `platform`, `types`, `io`, `code`, `json`, `email`, `test`, `queue`,
`select`, `signal`, `socket`, `string`, `time`, `token`, `copy`, `random`, `secrets`, `statistics`.

## Phương án đã cân nhắc

- **Giữ `platform/` và không bao giờ thêm `__init__.py`.** Bị loại: buộc phải import bằng thủ thuật
  `sys.path`, và cái mìn vẫn nằm đó chờ người sau vô tình thêm `__init__.py`. Đổi tên là dứt điểm.
- **`control_plane/`.** Cân nhắc nghiêm túc, đúng thuật ngữ trong roadmap §1. Bị loại vì dài và ít
  dùng khi gõ lệnh hằng ngày (`python -m control_plane.cli`).
- **`ctl/`, `cp/`.** Bị loại: ngắn nhưng tối nghĩa.
