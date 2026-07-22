# ADR-0013: Secrets nằm trong `.env` đã gitignore — giới hạn đã biết, không phải rò rỉ Git

- **Status:** Accepted *(giới hạn của lab — chặn lên production)*
- **Date:** 2026-07-15
- **Deciders:** Phan Trường

## Bối cảnh

Stack cần khoảng 15 giá trị bí mật: mật khẩu Postgres cho 3 user, mật khẩu ClickHouse, key MinIO,
mật khẩu Grafana, và một App Password Gmail cho fraud-notifier.

**Cần đính chính một hiểu nhầm.** Vài ghi chép nội bộ trước đây mô tả tình trạng này là *"sự cố bảo
mật P0: `.env` chứa mật khẩu thật và App Password Gmail thật được commit vào Git"*, kèm khuyến nghị
thu hồi khẩn cấp và chạy `git filter-repo` để xoá khỏi lịch sử.

**Điều đó không đúng.** Kiểm chứng:

```bash
$ git ls-files | grep -i env
# không kết quả — .env không được Git theo dõi

$ git log --all -- .env
# không kết quả — .env chưa từng được commit trong bất kỳ nhánh nào

$ head -1 .gitignore
.env
```

`.env` được gitignore ngay từ commit đầu và chưa bao giờ vào lịch sử Git. Không có gì để thu hồi khỏi
Git, và **không** cần `git filter-repo`.

Ghi ADR này lại để đính chính khỏi lan tiếp — chạy `git filter-repo` để "sửa" một vấn đề không tồn tại
sẽ viết lại lịch sử và phá mọi bản clone, hoàn toàn vô ích.

## Quyết định

Giữ secret trong `.env` local đã gitignore cho môi trường lab. Ghi nhận đây là **giới hạn đã biết** cần
xử lý **trước khi** lên production, không phải sự cố cần phản ứng khẩn.

Nguyên tắc bổ trợ đã được tuân thủ tốt: **không mật khẩu nào nằm trong file cấu hình được commit.**
Mọi connector JSON và catalog Trino đều tham chiếu biến môi trường:

```json
"database.password": "${env:REPLICATION_PASSWORD}"
```
```properties
connection-password=${ENV:CLICKHOUSE_PASSWORD}
```

Nhờ `EnvVarConfigProvider` của Kafka Connect và cơ chế thay thế env của Trino. Đây là **thực hành
tốt**, giữ nguyên.

## Hệ quả

**Rủi ro thật (hẹp hơn "rò rỉ Git", nhưng có thật):**

| Rủi ro | Chi tiết |
|---|---|
| Secret plaintext trên đĩa | Không secret manager, không xoay vòng. Ai đọc được máy là đọc được hết. |
| Lộ qua `docker inspect` | Compose truyền secret bằng biến môi trường → hiện trong `docker inspect` và có thể lọt vào log. |
| Không có quét secret trong CI | Chỉ `.gitignore` chặn. Ai đó `git add -f .env` là lọt, không có lưới thứ hai. |
| App Password Gmail | Là credential thật của tài khoản thật. Rủi ro cao nhất trong nhóm, vì nó ra ngoài phạm vi lab. |

**Đường lên production** (Pha 0 của [lộ trình](../roadmap/BDP-metadata-driven-roadmap.md)):
1. **SOPS + age** (đơn giản, hợp Git — commit file đã mã hoá) hoặc **HashiCorp Vault** (nếu cần secret
   động).
2. Metadata chỉ chứa `secret_ref`, **không bao giờ** chứa giá trị.
3. Thêm **quét secret vào CI** (gitleaks / trufflehog) làm lưới thứ hai sau `.gitignore`.
4. Chuyển sang Docker secrets (mount file) thay vì biến môi trường, để tránh `docker inspect`.
5. Xoay vòng App Password Gmail định kỳ; cân nhắc dùng SMTP relay riêng thay tài khoản cá nhân.

## Phương án đã cân nhắc

- **Commit `.env` cho tiện.** Bị loại — và không bao giờ được làm. `.gitignore` đã đúng từ đầu.
- **`git filter-repo` để xoá `.env` khỏi lịch sử.** **Không áp dụng** — nó chưa từng ở trong lịch sử.
  Chạy lệnh này sẽ viết lại lịch sử và phá mọi bản clone, đổi lấy con số không.
- **SOPS ngay từ đầu.** Bị hoãn: thêm bước giải mã cho một dự án chạy trên đúng một laptop. Đúng cho
  production, thừa cho lab. Đây là đánh đổi có ý thức, không phải bỏ sót.
- **Docker secrets.** Bị hoãn: đúng cho Swarm/Kubernetes; với Compose thì phải mount file và sửa mọi
  service để đọc từ file thay vì env.
