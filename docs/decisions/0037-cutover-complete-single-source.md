# ADR-0037: Chốt cutover — `metadata/` là nguồn sự thật duy nhất (Pha 8)

- **Status:** Accepted — cutover xong, runbook viết, audit không còn file viết tay song song
- **Date:** 2026-07-20
- **Deciders:** Phan Trường

## Bối cảnh

Lộ trình metadata-driven ([ADR-0014](0014-adopt-metadata-driven-roadmap.md)) đặt đích: mọi "sự thật về dữ
liệu" tập trung ở `metadata/`, sinh mọi artifact từ đó, xoá bản chép tay. Qua Pha 2–7, cutover diễn ra
**dần** (mỗi thành phần: chứng minh bản sinh khớp byte-exact → xoá bản viết tay tương ứng). Pha 8 chốt lại.

## Quyết định — tuyên bố cutover hoàn tất

**Audit (2026-07-20):** không còn file viết tay song song với bản sinh. Đã xoá dần: 4 job Flink print-sink,
`lane1_dashboard.py`, `lane3_fraud_detection.py`, `enrich_transactions.py`, `build_gold_layer.py`, và cuối
cùng `trino/iceberg.properties.bak` (di sản hadoop-catalog). Còn lại đúng những thứ nên viết tay: 3 runner
generic (`metric_runner`/`fraud_runner`/`medallion_runner` — engine tham số hoá, không phải artifact),
`clickhouse/init/03_dlq.sql` (infra tĩnh), migration (bất biến theo bản chất).

**Runbook** ([`docs/guide/runbook.md`](../guide/runbook.md)): mọi tác vụ (thêm cột/bảng/metric/connection,
breaking change, migration, rollback, backfill, quality) quy về "sửa metadata + chạy" — kèm tham chiếu lệnh
+ gotchas đã trả bằng thời gian debug.

**19 artifact** sinh từ `metadata/`, `check` 19/19 byte-exact, CI gác drift + BACKWARD ở mọi PR. Thêm cột =
sửa một contract (trước: tối đa 6 file Flink + 3 ClickHouse + Spark + ES). Sprawl #1–#13 đóng.

## Hệ quả

**Dễ hơn:** một nơi để sửa; thay đổi review được bằng diff có nghĩa + plan hệ quả; onboard bằng runbook.

**Ngoài phạm vi metadata-driven (nợ riêng, có chủ đích):**
- **Bảo mật:** secret vẫn plaintext `.env` (đã gitignore, [ADR-0013](0013-secrets-in-gitignored-env.md));
  service tắt auth. → Pha bảo mật (Vault/SOPS + `secret_ref`, bật auth).
- **HA:** single-node toàn bộ (Kafka RF=1, 1 Spark worker); Flink/Spark chết là mất job; iceberg-rest lưu
  catalog RAM. → Pha robustness.
- **Airflow e2e:** DAG load OK trong Airflow thật ([ADR-0031](0031-airflow-dag-from-metadata.md)); chạy task
  spark-submit xuyên suốt cần dựng stack Spark — làm khi cần.
- Silver full-refresh (`overwrite`) thay vì incremental.

Đây không phải thiếu sót của cutover — là các trục khác (bảo mật/HA/runtime) mà metadata-driven không giải,
tách phase riêng để không trộn mối lo.

## Phương án đã cân nhắc

- **Xoá luôn `clickhouse/init/*.sql`, gom hết vào migration.** Loại: init là baseline sinh-từ-contract
  (idempotent, cho bảng mới) — vai trò khác migration; 11 doc tham chiếu, churn lớn ([ADR-0032](0032-versioned-migration-clickhouse.md)).
- **Chốt cutover kèm luôn bảo mật/HA.** Loại: khác trục, tách phase để mỗi mối lo làm tới nơi.
