# Metadata-driven — Trình tự triển khai (từ đâu tới đâu)

> File này giải thích **đã làm theo thứ tự nào**, mỗi phần chia ra các bước gì. Không đi sâu code —
> chi tiết nằm trong `docs/decisions/` (ADR) và code trong `dataplatform/`. Đọc kèm:
> `docs/roadmap/BDP-metadata-driven-roadmap.md` (cái đích) + `docs/architecture/BDP-current-state.md` (điểm xuất phát).

---

## Phần A — Tư duy chủ đạo (áp cho mọi pha)

Toàn bộ dự án chỉ lặp lại một khuôn 6 bước ("strangler fig" — bóp nghẹt dần cái cũ):

```
1. Viết contract    — khai "sự thật về dữ liệu" một lần vào metadata/*.yaml
2. Viết generator   — code sinh ra artifact (JSON/SQL/DAG...) từ contract
3. Check byte-exact — so bản sinh với bản viết tay đang có; phải khớp từng byte
4. Cắt chuyển       — khi đã khớp: cho hệ thống dùng bản sinh (deployer apply)
5. Xóa bản cũ       — xóa file viết tay tương ứng (không để song song)
6. Verify + ADR     — kiểm chứng runtime + ghi một ADR (vì sao làm vậy)
```

**Vì sao khuôn này an toàn:** bước 3 chứng minh bản sinh == bản cũ *trước khi* dám thay. Không "viết lại
từ đầu rồi cầu mong nó chạy". `cli check` chính là cái cổng đó — chừng nào chưa khớp thì chưa cắt chuyển.

Ba lệnh xương sống dùng suốt:
- `python -m dataplatform.cli write` — sinh mọi artifact từ metadata.
- `python -m dataplatform.cli check` — so bản sinh vs file trên đĩa (gác "drift").
- `python -m dataplatform.deployers.<x> apply` — áp artifact lên hệ thống sống (idempotent).

---

## Phần B — Điểm xuất phát (Pha 0)

Hệ thống đã chạy được (CDC → Kafka → Flink → ClickHouse/ES/lake → Trino), nhưng "sự thật về một bảng"
(cột gì, khóa gì, vào topic nào) bị **chép tay ở ~10 nơi**. Đổi một cột = sửa nhiều file, dễ sót → gọi là
"metadata sprawl". Mục tiêu cả dự án: gom về một nơi (`metadata/`), sinh mọi thứ từ đó.

**Bước làm:** dựng khung `dataplatform/` (control plane) + `metadata/` (nơi khai contract) +
`dataplatform/schemas/*.json` (JSON Schema validate contract). Chốt "registry là YAML trong Git" (ADR-0015).

---

## Phần C — Các pha, theo đúng thứ tự đã làm

### Pha 1 — Mô hình hóa & mã hóa contract
1. Định nghĩa **dataset contract** (`metadata/datasets/*.yaml`): urn, layer, owner, source, columns (name/type/
   nullable/pk/pii), sinks.
2. Viết `registry.py` (đọc + validate contract) + schema JSON.
3. Lát cắt dọc đầu tiên để chứng minh khuôn chạy: sinh **5 config ES sink** từ contract (loạt ADR 0011/0018).

### Pha 2 — Ingestion sinh từ contract
1. Sinh **Debezium connector + publication SQL** từ cùng một danh sách dataset CDC (diệt sprawl #2/#3, ADR-0018).
2. Sinh **DDL ClickHouse** cho metric (bảng đích + Kafka engine + Materialized View — 3 thứ từ 1 `columns`, ADR-0019).
3. Sinh **bản kê topic Kafka** (`kafka/topics.json` + `create-topics.sh`) để tắt được `auto.create.topics` an toàn (ADR-0020).
4. Viết **deployer connector idempotent** (`connectors.py`) — biến metadata thành "load-bearing": áp thật lên Kafka Connect (ADR-0021).
5. **Cắt chuyển**: tắt `auto.create.topics`, chạy bằng bản sinh. Cắm `cli check` vào **CI** (mọi PR gác drift).
6. Viết **verifier ngược**: so contract vs schema thật ở Postgres/ClickHouse (ADR-0022) + so **Avro trên dây** (Schema Registry).

### Pha 3 — Flink (streaming) sinh từ metadata
1. Khai **pipeline spec** cho metric (dimensions/aggregations bằng biểu thức).
2. Viết **metric runner khai báo**: sinh Flink SQL từ spec (diệt khối `ROW<...>` chép tay, ADR-0023).
3. **Cắt chuyển** + xóa `lane1_dashboard.py`; tham số hóa **fraud runner** từ metadata. Xóa các job Flink cũ.

### Pha 4 — ClickHouse serving
1. Hoàn chỉnh generator DDL metric (đã mở ở Pha 2) — mọi bảng serving suy từ contract.
2. Verify: `verifiers/clickhouse_schema` — bảng live khớp contract, 0 drift.

### Pha 5 — Spark medallion (lakehouse)
1. Khai **batch spec** (`metadata/pipelines/batch/*.yaml`): inputs + SQL + output columns (mô hình dbt, ADR-0024).
2. Viết **medallion runner** (Spark, SQL-in-spec) cho Silver.
3. Thêm **3 Gold spec** + **Iceberg spec**; viết **deployer chạy theo thứ tự phụ thuộc** (bronze→silver→gold/iceberg).
4. **Cắt chuyển**: chứng minh parity (row count khớp job cũ) → xóa `enrich_transactions.py`, `build_gold_layer.py`.

### Pha 6 — Federation + Catalog/Lineage
1. Thêm **connection registry** (`metadata/connections/*.yaml`); sinh **Trino catalog** từ đó (ADR-0025).
2. Verify **federation runtime**: query chéo Postgres × ClickHouse × Iceberg trong 1 câu Trino.
3. Sinh **lineage graph + data catalog** thuần từ metadata (`lineage/graph.json` + `LINEAGE.md`, ADR-0026).
4. Dựng **OpenMetadata**; viết `deployers/openmetadata.py` nạp catalog từ `graph.json` (table + PII tag + lineage, ADR-0027).
5. Tăng dần: tạo table cho **đích sink ngoài** (ES/CH/S3) → lineage đủ 25 cạnh; **lineage cấp cột cho Spark**
   (parse SQL bằng sqlglot, ADR-0028) → đẩy vào OM; **encode connection non-Trino** (kafka/es/s3/schema-registry
   vào registry, generator đọc endpoint từ đó thay vì hardcode, ADR-0029).

### Pha 7 — Orchestration, CI/CD & Governance
1. **CI GitOps**: `cli plan` (hệ quả artifact khi merge) + `cli compat` (gate BACKWARD chặn breaking change) → cắm CI (ADR-0030).
2. **Orchestration**: sinh **Airflow DAG** từ phụ thuộc input/output của batch spec (silver→gold/iceberg, ADR-0031).
3. **Versioned migration** ClickHouse: runner + `migrations/clickhouse/NNNN_*.sql` + sổ cái `schema_migrations`,
   bỏ "init-once" (ADR-0032).
4. **Data quality gate**: `verifiers/quality.py` + `metadata/quality/*.yaml` — not_null/unique tự suy từ contract
   + range/accepted_values, chạy SQL trên dữ liệu thật, fail chặn promote (ADR-0033).
5. **Rollback**: `connectors --ref <git-ref>` áp lại desired state đã commit ở ref cũ (ADR-0034).
6. **RBAC & audit**: `.github/CODEOWNERS` theo vùng metadata + `owner` trong contract + audit Git/lineage (ADR-0035).
7. **Iceberg migration**: quyết định dùng **native evolution** (ALTER + snapshot + rollback_to_snapshot), không runner (ADR-0036).
8. **Boot Airflow** thật — DAG load OK qua DagBag (0 import error).

### Pha 8 — Cắt chuyển & vận hành hóa
1. **Audit cutover cuối**: không còn file viết tay song song bản sinh (xóa `trino/iceberg.properties.bak`).
2. Viết **runbook** (`docs/guide/runbook.md`): mọi tác vụ (thêm cột/bảng/metric/connection, breaking change,
   migration, rollback, backfill, quality) quy về "sửa metadata + chạy" + bảng gotchas.
3. Chốt: **một nơi để sửa — `metadata/`** (ADR-0037).

---

## Phần D — Kết quả & cách kiểm lại

- `metadata/` là nguồn sự thật duy nhất → **19 artifact** sinh tự động, `cli check` **19/19 byte-exact**.
- Thêm 1 cột = sửa **1 contract** (trước: tối đa 6 file Flink + 3 ClickHouse + Spark + ES).
- Đã verify LIVE: avro trên dây khớp contract, ClickHouse 0 drift, Spark 1072 rows, federation 3 nguồn,
  OM catalog 24 table, quality 66 check, migration idempotent, Airflow DAG load, rollback.

**Ngoài phạm vi metadata-driven** (trục khác, chưa làm): bảo mật (secret manager + auth service),
HA/robustness, Silver incremental.

## Phần E — Đọc tiếp ở đâu

| Muốn hiểu | Đọc |
|---|---|
| Vì sao mỗi quyết định | `docs/decisions/README.md` (index 37 ADR) |
| Cái đích + từng pha | `docs/roadmap/BDP-metadata-driven-roadmap.md` |
| Điểm xuất phát (sprawl) | `docs/architecture/BDP-current-state.md` |
| Vận hành hằng ngày | `docs/guide/runbook.md` |
| Code control plane | `dataplatform/` (generators / deployers / verifiers) |
