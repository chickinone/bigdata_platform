# ADR-0034: Rollback deployer — áp lại desired state từ git ref (Pha 7)

- **Status:** Accepted — `connectors --ref` đọc đúng config lịch sử (verify offline: HEAD == generator)
- **Date:** 2026-07-20
- **Deciders:** Phan Trường

## Bối cảnh

Deployer connector đã idempotent (áp desired state, ADR-0021), nhưng khi một thay đổi hỏng thì "quay lui"
là gì? Không có cơ chế rollback tường minh. GitOps cần: đổi hỏng → áp lại trạng thái tốt trước đó, nhanh.

## Quyết định

Rollback = **áp lại desired state của một git ref cũ**. Vì `check` bảo đảm artifact đã commit == metadata,
và deployer áp desired state, nên "áp lại artifact ở ref X" = "đưa hệ thống về đúng trạng thái ref X".

`deployers/connectors.py` thêm `--ref <git-ref>`:
- `desired_connectors(ref)` đọc config connector đã COMMIT ở ref đó (`git show <ref>:<path>`) thay vì chạy
  generator trên working tree.
- `plan --ref X` cho thấy rollback sẽ đổi gì (so với Connect đang chạy); `apply --ref X` PUT lại config của X.

Không cần checkout/worktree hay chạy lại generator ở ref — artifact commit là desired state đã đóng băng
của ref đó. Rollback nhanh, không đụng working tree.

### Vì sao đọc artifact commit, không regenerate ở ref

Regenerate cần checkout metadata ở ref + chạy generator (chậm, đụng working tree). Artifact đã commit +
`check` đã bảo đảm nó == metadata ở ref → đọc thẳng là đủ và chính xác. Một lợi ích phụ: rollback vẫn chạy
kể cả khi generator sau này đổi (artifact cũ vẫn là sự thật của thời điểm đó).

## Kiểm chứng

- **Offline** (không cần Connect): `desired_connectors("HEAD")` (đọc artifact git) == `desired_connectors()`
  (chạy generator) — 7 connector, 0 lệch config (chuẩn hoá thứ tự list). Chứng minh đường rollback đọc
  đúng desired state lịch sử.
- PUT thật lên Kafka Connect cần Connect sống (phiên riêng) — cùng đường `apply` đã verify ở ADR-0021.

## Hệ quả

**Dễ hơn:** `connectors apply --ref <commit-tốt>` = rollback một lệnh, idempotent, có `plan` xem trước.

**Khó hơn / phải chấp nhận:**
- Mới làm cho connector (deployer load-bearing nhất). Spark/Flink/OM rollback theo cùng khuôn (đọc
  artifact/spec ở ref) — increment sau.
- Rollback CONFIG, không rollback dữ liệu đã ghi (đó là việc của backfill/snapshot). Với Iceberg, rollback
  dữ liệu là native (ADR-0036).
- Cần artifact được commit (đã có — `check` ép vậy).

## Phương án đã cân nhắc

- **`git revert` + `apply` thủ công.** Vẫn làm được, nhưng `--ref` gọn hơn + có `plan` xem trước, không
  đụng working tree.
- **Lưu snapshot desired state riêng.** Loại: Git đã là lịch sử desired state; thêm store là trùng lặp.
- **Checkout/worktree ở ref rồi regenerate.** Loại: chậm + đụng working tree; artifact commit đã đủ.
