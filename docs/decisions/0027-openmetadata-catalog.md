# ADR-0027: OpenMetadata làm catalog UI — nạp từ metadata (Pha 6)

- **Status:** Accepted — OpenMetadata chạy được, catalog nạp từ `graph.json`, verify UI
- **Date:** 2026-07-19
- **Deciders:** Phan Trường

## Bối cảnh

Pha 6 cần một **catalog UI** (discovery + lineage + governance). Hai lựa chọn: DataHub vs OpenMetadata.

## Quyết định — OpenMetadata

Chọn **OpenMetadata** cho dự án này vì: footprint vận hành gọn hơn tương đối (một server thay vì nhiều
mảnh rời của DataHub), **governance/PII mạnh bẩm sinh** (khớp phát hiện "PII chảy vào lake" ở ADR-0026), và
hợp người đang học. DataHub có lineage đẳng cấp hơn (impact analysis) nhưng nặng + phức tạp hơn — để dành
nếu lên quy mô nhiều team.

Dùng **compose chính thức** của OpenMetadata 1.12.6 (bản postgres), **không** tự chế 555 dòng env. Bỏ
Airflow bằng cách chỉ start `openmetadata-server` (kéo postgres+es+migrate, không kéo ingestion).

### Nạp catalog từ metadata, không gõ tay trên UI

`deployers/openmetadata.py` đọc `lineage/graph.json` (ADR-0026) và PUSH qua REST: service → database →
schema (theo layer) → table (mỗi dataset/lake node) + cột + **tag PII** + **lineage**. Giữ đúng "Git là
nguồn sự thật, catalog là nơi tra cứu" — metadata đổi thì chạy lại deployer.

## Máy có đủ không — đo thật

| | |
|---|---|
| Host RAM | 15.3 GB (chỉ ~0.2 GB trống khi full stack chạy) |
| WSL/Docker cấp | 12 GB; full stack (~19 container) dùng **~8 GB** |
| OpenMetadata (server+ES+postgres, bỏ Airflow) | **~3 GB** |

**Kết luận: Không đủ chạy cả hai cùng lúc.** Nhưng **đủ nếu tạm dừng stack chính** (giải phóng ~8GB) —
OM chạy ổn ở ~3GB. Vì deployer PUSH `graph.json` (không auto-ingest từ engine sống), stack chính không cần
bật lúc nạp catalog. Nên catalog là một **phiên riêng**: `docker compose stop` → bật OM → nạp → đảo lại.

## Kiểm chứng (đo thật qua API OM)

Nạp xong, verify qua API OM:
- **24 table** trong `bdp.bank` = 9 dataset + 5 lake node + **10 đích sink ngoài** (4 ClickHouse
  + 5 Elasticsearch + 1 S3). Sink ngoài nay là table thật nên lineage không còn bị cụt.
- **Tag PII đúng cột**: `oltp.customers` có `full_name/email/phone` gắn `PII.Sensitive`.
- **Lineage đủ 25/25 cạnh** (trước chỉ giữ 12 cạnh nội bộ). Ví dụ `elasticsearch.fraud_alerts`
  có upstream nối ngược về `alerts.fraud_alerts` → `transactions`.
- **Lineage cấp cột (83 liên kết)** đẩy vào `lineageDetails.columnsLineage` (nguồn: `column_lineage`
  của graph, ADR-0028). Lake table nay tạo với **cột thật** (không còn placeholder `_`) nên đích cột
  tồn tại. Verify trên UI: `silver.enriched_transactions.customer_name` ← `oltp.customers.full_name`
  — chuỗi **PII lộ rõ tới từng cột**.

### Cú vấp cần nhớ: Elasticsearch phải sống thì lineage mới nạp được

Server OM lưu entity ở postgres, nên **table PUT được ngay cả khi ES chết** — nhưng bước
`PUT /api/v1/lineage` ghi vào chỉ mục search, ES chết là **500 `[elasticsearch]`**. Ở máy này
container `openmetadata_elasticsearch` từng bị **OOM-kill (exit 137)**. Trước khi chạy deployer
phải chắc ES `yellow`/`green`: `curl -s localhost:9200/_cluster/health`. Nếu chết thì
`docker compose -f openmetadata/docker-compose-openmetadata.yml up -d elasticsearch` (heap 1GB,
đủ chỗ khi stack chính đang dừng).

## Hệ quả

**Dễ hơn:** có UI tra cứu + search + lineage đồ hoạ; nạp lại một lệnh khi metadata đổi.

**Khó hơn / phải chấp nhận:**
- Trên máy này catalog là **công cụ phiên-riêng** (dừng stack chính trước). Máy nhiều RAM hơn thì chạy song song được.
- ES là mắt xích bắt buộc cho lineage; RAM eo hẹp nên nó dễ OOM (xem cú vấp ở phần Kiểm chứng).
- Push qua REST thô (không cài SDK `openmetadata-ingestion` cho nhẹ). Cột lake nay có tên thật nhưng kiểu
  vẫn gán `STRING` (schema chi tiết ở contract/output.columns). Table **sink ngoài** vẫn chỉ 1 cột placeholder
  — chỉ để **neo lineage mức bảng**, chưa mô tả cột.

## Phương án đã cân nhắc

- **DataHub.** Loại cho dự án này: nặng hơn + nhiều mảnh, dốc hơn để vận hành; lợi thế lineage/impact chưa
  bù được chi phí ở quy mô lab.
- **Chạy OM song song full stack.** Bất khả thi trên 15.3GB RAM (đo thật).
- **Tự chế compose.** Loại: OM có 555 dòng env, dễ sai — dùng bản chính thức.
- **Chỉ giữ `LINEAGE.md`/`graph.json`.** Vẫn giữ làm catalog nhẹ bổ sung (không tốn container); OM là bản UI đầy đủ.
