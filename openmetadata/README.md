# OpenMetadata — data catalog & lineage UI (Pha 6)

> Máy này (15.3GB RAM) **không đủ chạy OpenMetadata CÙNG stack chính**. Chạy catalog là một
> phiên **riêng**: tạm dừng stack chính, bật OpenMetadata, xong thì đảo lại. Xem [ADR-0027](../docs/decisions/0027-openmetadata-catalog.md).

## Chạy

```bash
# 1. Giải phóng RAM: tạm dừng stack chính (giữ nguyên volume/state)
docker compose stop

# 2. Bật OpenMetadata (chỉ server + postgres + es + migrate; BỎ Airflow bằng cách chỉ start server)
docker compose -f openmetadata/docker-compose-openmetadata.yml up -d openmetadata-server

# 3. Nạp catalog TỪ metadata (Git là nguồn sự thật, không gõ tay trên UI)
python -m dataplatform.cli write                       # đảm bảo lineage/graph.json mới nhất
python -m dataplatform.deployers.openmetadata apply    # push dataset + PII + lineage

# 4. Mở UI: http://localhost:8585   (admin@open-metadata.org / admin)
```

## Đảo lại (chạy pipeline chính)

```bash
docker compose -f openmetadata/docker-compose-openmetadata.yml stop
docker compose start                                   # khôi phục stack chính
python -m dataplatform.deployers.flink_metrics apply   # resubmit Flink runner (nợ HA)
```

## Ghi chú

- `docker-compose-openmetadata.yml` là **compose CHÍNH THỨC** của OpenMetadata 1.12.6 (bản postgres),
  tải nguyên bản. Ta chỉ **không start** service `ingestion` (Airflow) để tiết kiệm ~1-2GB.
- Catalog nạp qua REST API từ `lineage/graph.json` — nếu metadata đổi, chạy lại bước 3.
- RAM khi chạy: server ~1.2GB + ES ~1.7GB + postgres ~0.1GB ≈ **3GB** (vừa chỗ khi stack chính dừng).
