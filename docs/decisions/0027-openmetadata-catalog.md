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

### Nạp catalog TỪ metadata, không gõ tay trên UI

`deployers/openmetadata.py` đọc `lineage/graph.json` (ADR-0026) và PUSH qua REST: service → database →
schema (theo layer) → table (mỗi dataset/lake node) + cột + **tag PII** + **lineage**. Giữ đúng "Git là
nguồn sự thật, catalog là nơi tra cứu" — metadata đổi thì chạy lại deployer.

## Máy có đủ không — ĐO THẬT

| | |
|---|---|
| Host RAM | 15.3 GB (chỉ ~0.2 GB trống khi full stack chạy) |
| WSL/Docker cấp | 12 GB; full stack (~19 container) dùng **~8 GB** |
| OpenMetadata (server+ES+postgres, bỏ Airflow) | **~3 GB** |

**Kết luận: KHÔNG đủ chạy cả hai cùng lúc.** Nhưng **đủ nếu tạm dừng stack chính** (giải phóng ~8GB) —
OM chạy ổn ở ~3GB. Vì deployer PUSH `graph.json` (không auto-ingest từ engine sống), stack chính không cần
bật lúc nạp catalog. Nên catalog là một **phiên riêng**: `docker compose stop` → bật OM → nạp → đảo lại.

## Kiểm chứng

Nạp xong, verify qua API OM:
- **14 table** trong `bdp.bank` (9 dataset + 5 lake node).
- **Tag PII đúng cột**: `customers` có `full_name/email/phone` gắn `PII.Sensitive`.
- **Lineage**: `transactions` → 10 cạnh downstream (4 metric + fraud + medallion silver→gold/iceberg).

## Hệ quả

**Dễ hơn:** có UI tra cứu + search + lineage đồ hoạ; nạp lại một lệnh khi metadata đổi.

**Khó hơn / phải chấp nhận:**
- Trên máy này catalog là **công cụ phiên-riêng** (dừng stack chính trước). Máy nhiều RAM hơn thì chạy song song được.
- Đích sink ngoài (ES index, ClickHouse table, S3) **chưa tạo thành table** → 13/25 cạnh lineare tới chúng bị bỏ
  (giữ 12 cạnh nội bộ dataset↔lake). Thêm chúng là increment sau.
- Push qua REST thô (không cài SDK `openmetadata-ingestion` cho nhẹ). Cột lake chưa có schema chi tiết.

## Phương án đã cân nhắc

- **DataHub.** Loại cho dự án này: nặng hơn + nhiều mảnh, dốc hơn để vận hành; lợi thế lineage/impact chưa
  bù được chi phí ở quy mô lab.
- **Chạy OM song song full stack.** Bất khả thi trên 15.3GB RAM (đo thật).
- **Tự chế compose.** Loại: OM có 555 dòng env, dễ sai — dùng bản chính thức.
- **Chỉ giữ `LINEAGE.md`/`graph.json`.** Vẫn giữ làm catalog nhẹ bổ sung (không tốn container); OM là bản UI đầy đủ.
