# ADR-0035: RBAC & audit — CODEOWNERS + owner trong contract (Pha 7)

- **Status:** Accepted — `.github/CODEOWNERS` theo vùng metadata; audit qua Git + `owner` contract + lineage
- **Date:** 2026-07-20
- **Deciders:** Phan Trường

## Bối cảnh

Metadata-driven làm mọi thay đổi đi qua Git, nhưng chưa có **"ai được sửa contract nào"** và dấu vết
quản trị. Hạ nguồn phụ thuộc contract; sửa bừa một dataset OLTP có thể phá nhiều pipeline. Cần RBAC + audit.

## Quyết định

Không dựng hệ RBAC riêng — **tận dụng GitHub + metadata sẵn có**:

1. **`.github/CODEOWNERS`** chia quyền review theo VÙNG metadata: `metadata/datasets/`, `connections/`,
   `pipelines/`, `quality/`, `dataplatform/`, `migrations/`, `docs/decisions/`. Kết hợp branch protection
   "Require review from Code Owners" trên `main` → đổi một vùng BẮT BUỘC có review của owner vùng đó. Đây là
   "ai được sửa contract nào", thực thi bằng GitHub.
2. **`owner:` trong mỗi dataset contract** (đã có) = ai SỞ HỮU dữ liệu (nghiệp vụ). OpenMetadata + lineage
   phơi bày owner → tra cứu được.
3. **Audit trail** = Git history (ai đổi gì, khi nào, review bởi ai) + ADR (vì sao) + lineage (ảnh hưởng
   tới đâu). Không cần audit log riêng.

Lab hiện một người (`@chickinone`); cấu trúc CODEOWNERS theo domain để thêm team = đổi owner từng dòng
(vd nhóm risk sở hữu `metadata/datasets/oltp/`).

## Hệ quả

**Dễ hơn:** phân quyền + audit "miễn phí" từ GitHub + metadata; không hạ tầng RBAC riêng. Đổi contract có
review bắt buộc đúng người.

**Khó hơn / phải chấp nhận:**
- CODEOWNERS chỉ mạnh khi bật **branch protection** trên GitHub (cấu hình repo, ngoài Git) + có nhiều người.
  Solo thì nó là khai báo quyền sở hữu (giá trị khi lên team).
- RBAC ở tầng RUNTIME (ai query được ClickHouse/Trino/ES) là chuyện khác — hiện các service tắt auth (nợ
  production #12 ở current-state), xử ở Pha bảo mật riêng.

## Phương án đã cân nhắc

- **Hệ RBAC riêng (OPA, DB phân quyền).** Loại: nặng, thừa cho "ai sửa contract" — CODEOWNERS + branch
  protection đã đúng việc.
- **Audit log riêng.** Loại: Git đã là audit log bất biến, ký được, review được. Thêm store là trùng lặp.
